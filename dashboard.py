import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
from cryptography.fernet import Fernet
import tempfile
import os

#checking if the file is encrypted

class LinkedInDashboard:
    def __init__(self, encrypted_db_path="linkedin_data.encrypted.db"):
        """Initialize dashboard with encrypted database."""
        self.encrypted_db_path = encrypted_db_path
        self.temp_db_path = None
        
    def decrypt_database(self):
        """Decrypt the database to a temporary file."""
        try:
            # Get key from Streamlit secrets
            if 'db_key' not in st.secrets:
                raise Exception("Database key not found in secrets")
            
            key = st.secrets['db_key'].encode()
            fernet = Fernet(key)
            
            # Read encrypted database
            with open(self.encrypted_db_path, 'rb') as file:
                encrypted_data = file.read()
            
            # Decrypt data
            decrypted_data = fernet.decrypt(encrypted_data)
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            self.temp_db_path = temp_file.name
            
            # Write decrypted data to temporary file
            with open(self.temp_db_path, 'wb') as file:
                file.write(decrypted_data)
                
        except Exception as e:
            st.error(f"Error decrypting database: {str(e)}")
            raise
    
    def get_connection(self):
        """Create and return a database connection."""
        if not self.temp_db_path:
            self.decrypt_database()
        return sqlite3.connect(self.temp_db_path)
    
    def cleanup(self):
        """Clean up temporary database file."""
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.unlink(self.temp_db_path)

    def load_data(self):
        """Load data from database into DataFrames."""
        conn = self.get_connection()
        
        # Load profiles with school names (using company as school)
        query = """
            SELECT 
                p.*,
                s.name as school_name
            FROM profiles p
            JOIN schools s ON p.school_id = s.id
        """
        self.df = pd.read_sql_query(query, conn)
        
        # Get unique values for filters
        self.schools = sorted(self.df['company'].unique())  # Use company instead of school_name
        self.titles = sorted(self.df['title'].dropna().unique())
        self.locations = sorted(self.df['location'].dropna().unique())
        
        conn.close()

    def run(self):
        """Run the Streamlit dashboard."""
        st.set_page_config(page_title="LinkedIn Schools Dashboard", layout="wide")
        
        # Title and description
        st.title("LinkedIn Schools Data Explorer")
        st.markdown("Filter and analyze LinkedIn profiles from various schools")
        
        try:
            self.load_data()
            
            # Sidebar filters
            st.sidebar.header("Filters")
            
            # School filter
            selected_schools = st.sidebar.multiselect(
                "Select Schools",
                self.schools,
                default=[]
            )
            
            # Title filter with search
            title_search = st.sidebar.text_input("Search Job Titles")
            if title_search:
                filtered_titles = [t for t in self.titles if title_search.lower() in t.lower()]
            else:
                filtered_titles = self.titles
            
            selected_titles = st.sidebar.multiselect(
                "Select Job Titles",
                filtered_titles,
                default=[]
            )
            
            # Location filter
            selected_locations = st.sidebar.multiselect(
                "Select Locations",
                self.locations,
                default=[]
            )
            
            # Connection degree filter
            connection_degrees = sorted(self.df['connection_degree'].dropna().unique())
            selected_degrees = st.sidebar.multiselect(
                "Select Connection Degrees",
                connection_degrees,
                default=[]
            )
            
            # Apply filters
            filtered_df = self.df.copy()
            
            if selected_schools:
                filtered_df = filtered_df[filtered_df['company'].isin(selected_schools)]
            if selected_titles:
                filtered_df = filtered_df[filtered_df['title'].isin(selected_titles)]
            if selected_locations:
                filtered_df = filtered_df[filtered_df['location'].isin(selected_locations)]
            if selected_degrees:
                filtered_df = filtered_df[filtered_df['connection_degree'].isin(selected_degrees)]
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Profiles", len(filtered_df))
            with col2:
                st.metric("Schools", len(filtered_df['company'].unique()))
            with col3:
                st.metric("Unique Titles", len(filtered_df['title'].unique()))
            with col4:
                st.metric("Locations", len(filtered_df['location'].unique()))
            
            # Visualizations
            st.subheader("Data Analysis")
            tab1, tab2, tab3 = st.tabs(["Profile Distribution", "Job Titles", "Locations"])
            
            with tab1:
                # School distribution
                school_counts = filtered_df['company'].value_counts().reset_index()
                school_counts.columns = ['school', 'count']  # Rename columns
                fig1 = px.bar(
                    school_counts,
                    x='school',
                    y='count',
                    title="Profiles by School",
                    labels={'school': 'School', 'count': 'Number of Profiles'}
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with tab2:
                # Top job titles
                title_counts = filtered_df['title'].value_counts().head(20).reset_index()
                title_counts.columns = ['title', 'count']  # Rename columns
                fig2 = px.bar(
                    title_counts,
                    x='count',
                    y='title',
                    title="Top 20 Job Titles",
                    orientation='h'
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            with tab3:
                # Location distribution
                location_counts = filtered_df['location'].value_counts().head(10).reset_index()
                location_counts.columns = ['location', 'count']  # Rename columns
                fig3 = px.pie(
                    location_counts,
                    values='count',
                    names='location',
                    title="Top 10 Locations"
                )
                st.plotly_chart(fig3, use_container_width=True)
            
            # Data table
            st.subheader("Profile Data")
            
            # Search functionality
            search_term = st.text_input("Search profiles (name, title, or company)")
            if search_term:
                search_mask = (
                    filtered_df['name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['title'].str.contains(search_term, case=False, na=False) |
                    filtered_df['company'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[search_mask]
            
            # Display columns selection
            display_cols = ['name', 'title', 'company', 'location', 'school_name', 
                          'duration', 'connection_degree', 'mutual_connections', 'profile_url']
            
            st.dataframe(
                filtered_df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "profile_url": st.column_config.LinkColumn("Profile URL")
                }
            )
            
            # Export functionality
            if st.button("Export Filtered Data to CSV"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name="linkedin_filtered_data.csv",
                    mime="text/csv"
                )
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.error("Please make sure the database exists and contains the required tables.")
        finally:
            self.cleanup()

if __name__ == "__main__":
    dashboard = LinkedInDashboard()
    dashboard.run() 