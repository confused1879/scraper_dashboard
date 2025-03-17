import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
from cryptography.fernet import Fernet
import tempfile
import os

class LTADashboard:
    def __init__(self, encrypted_db_path="lta_data.encrypted.db"):
        """Initialize dashboard with encrypted database."""
        self.db_path = encrypted_db_path
        self.temp_db_path = None
        
    def decrypt_database(self):
        """Decrypt the database to a temporary file."""
        try:
            # Get key from Streamlit secrets
            if 'lta_db_key' not in st.secrets:
                raise Exception("LTA database key not found in secrets")
            
            key = st.secrets['lta_db_key'].encode()
            fernet = Fernet(key)
            
            # Read encrypted database
            with open(self.db_path, 'rb') as file:
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
            st.error(f"Error decrypting LTA database: {str(e)}")
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
        
        try:
            # Load clubs data
            self.clubs_df = pd.read_sql_query("""
                SELECT * FROM clubs
            """, conn)
            
            # Load teams data
            self.teams_df = pd.read_sql_query("""
                SELECT * FROM teams
            """, conn)
            
            # Load contacts data with team and club information
            self.contacts_df = pd.read_sql_query("""
                SELECT 
                    c.contact_id, c.name, c.phone, c.email,
                    tc.role, tc.team_id, tc.tournament_id,
                    t.team_name, t.school_name, t.gender,
                    cl.club_name, cl.location
                FROM contacts c
                JOIN team_contacts tc ON c.contact_id = tc.contact_id
                JOIN teams t ON tc.team_id = t.team_id AND tc.tournament_id = t.tournament_id
                LEFT JOIN clubs cl ON t.club_id = cl.club_id AND t.tournament_id = cl.tournament_id
            """, conn)
            
            # Load matches data
            self.matches_df = pd.read_sql_query("""
                SELECT * FROM matches
            """, conn)
            
            # Get unique values for filters
            self.club_names = sorted(self.clubs_df['club_name'].dropna().unique())
            self.school_names = sorted(self.teams_df['school_name'].dropna().unique())
            self.locations = sorted(pd.concat([
                self.clubs_df['location'].dropna(),
                self.contacts_df['location'].dropna()
            ]).unique())
            self.roles = sorted(self.contacts_df['role'].dropna().unique())
            self.genders = sorted(self.teams_df['gender'].dropna().unique())
            
        finally:
            conn.close()

    def run(self):
        """Run the Streamlit dashboard."""
        st.set_page_config(page_title="LTA Schools Dashboard", layout="wide")
        
        # Title and description
        st.title("LTA Schools Data Explorer")
        st.markdown("Filter and analyze LTA school and contact information")
        
        try:
            # Check if database exists
            if not os.path.exists(self.db_path):
                st.error(f"Database file {self.db_path} not found. Please run the lta_db_loader.py script first.")
                return
                
            self.load_data()
            
            # Sidebar filters
            st.sidebar.header("Filters")
            
            # School/Club filter
            filter_type = st.sidebar.radio(
                "Filter by:",
                ["All", "School", "Club"]
            )
            
            if filter_type == "School":
                selected_schools = st.sidebar.multiselect(
                    "Select Schools",
                    self.school_names,
                    default=[]
                )
            elif filter_type == "Club":
                selected_clubs = st.sidebar.multiselect(
                    "Select Clubs",
                    self.club_names,
                    default=[]
                )
            
            # Location filter
            selected_locations = st.sidebar.multiselect(
                "Select Locations",
                self.locations,
                default=[]
            )
            
            # Role filter
            selected_roles = st.sidebar.multiselect(
                "Select Roles",
                self.roles,
                default=[]
            )
            
            # Gender filter
            selected_genders = st.sidebar.multiselect(
                "Select Team Gender",
                self.genders,
                default=[]
            )
            
            # Apply filters to contacts dataframe
            filtered_contacts = self.contacts_df.copy()
            
            if filter_type == "School" and selected_schools:
                filtered_contacts = filtered_contacts[filtered_contacts['school_name'].isin(selected_schools)]
            elif filter_type == "Club" and selected_clubs:
                filtered_contacts = filtered_contacts[filtered_contacts['club_name'].isin(selected_clubs)]
            
            if selected_locations:
                filtered_contacts = filtered_contacts[filtered_contacts['location'].isin(selected_locations)]
            
            if selected_roles:
                filtered_contacts = filtered_contacts[filtered_contacts['role'].isin(selected_roles)]
            
            if selected_genders:
                filtered_contacts = filtered_contacts[filtered_contacts['gender'].isin(selected_genders)]
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Contacts", len(filtered_contacts))
            with col2:
                st.metric("Schools/Clubs", len(filtered_contacts[['school_name', 'club_name']].drop_duplicates()))
            with col3:
                st.metric("Teams", len(filtered_contacts['team_id'].unique()))
            with col4:
                st.metric("Locations", len(filtered_contacts['location'].dropna().unique()))
            
            # Create tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs([
                "Contact List", 
                "School/Club Distribution", 
                "Team Information",
                "Match Schedule"
            ])
            
            with tab1:
                st.subheader("Contact List")
                
                # Search functionality
                search_term = st.text_input("Search contacts (name, email, school, club)", key="contact_search")
                if search_term:
                    search_mask = (
                        filtered_contacts['name'].str.contains(search_term, case=False, na=False) |
                        filtered_contacts['email'].str.contains(search_term, case=False, na=False) |
                        filtered_contacts['school_name'].str.contains(search_term, case=False, na=False) |
                        filtered_contacts['club_name'].str.contains(search_term, case=False, na=False) |
                        filtered_contacts['team_name'].str.contains(search_term, case=False, na=False)
                    )
                    filtered_contacts = filtered_contacts[search_mask]
                
                # Display contacts in a table
                display_cols = [
                    'name', 'email', 'phone', 'role', 
                    'school_name', 'club_name', 'team_name', 
                    'gender', 'location'
                ]
                
                # Remove duplicate contacts (same person might be associated with multiple teams)
                deduplicated_contacts = filtered_contacts.drop_duplicates(subset=['contact_id'])
                
                st.dataframe(
                    deduplicated_contacts[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "email": st.column_config.LinkColumn("Email", display_text="Open Email"),
                        "name": st.column_config.TextColumn("Contact Name", width="medium"),
                        "school_name": st.column_config.TextColumn("School", width="medium"),
                        "club_name": st.column_config.TextColumn("Club", width="medium"),
                        "team_name": st.column_config.TextColumn("Team", width="medium"),
                        "role": st.column_config.TextColumn("Role", width="small"),
                        "gender": st.column_config.TextColumn("Gender", width="small"),
                        "location": st.column_config.TextColumn("Location", width="medium")
                    }
                )
                
                # Export functionality
                if st.button("Export Filtered Contacts to CSV"):
                    csv = deduplicated_contacts[display_cols].to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="lta_contacts.csv",
                        mime="text/csv"
                    )
            
            with tab2:
                st.subheader("School/Club Distribution")
                
                # Create distribution chart based on filter type
                if filter_type == "School" or filter_type == "All":
                    school_counts = filtered_contacts['school_name'].value_counts().reset_index()
                    school_counts.columns = ['school', 'count']
                    
                    if not school_counts.empty:
                        fig1 = px.bar(
                            school_counts,
                            x='school',
                            y='count',
                            title="Contacts by School",
                            labels={'school': 'School', 'count': 'Number of Contacts'}
                        )
                        st.plotly_chart(fig1, use_container_width=True)
                    else:
                        st.info("No school data available with current filters")
                
                if filter_type == "Club" or filter_type == "All":
                    club_counts = filtered_contacts['club_name'].value_counts().reset_index()
                    club_counts.columns = ['club', 'count']
                    
                    if not club_counts.empty:
                        fig2 = px.bar(
                            club_counts,
                            x='club',
                            y='count',
                            title="Contacts by Club",
                            labels={'club': 'Club', 'count': 'Number of Contacts'}
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("No club data available with current filters")
                
                # Location distribution
                location_counts = filtered_contacts['location'].value_counts().head(10).reset_index()
                location_counts.columns = ['location', 'count']
                
                if not location_counts.empty:
                    fig3 = px.pie(
                        location_counts,
                        values='count',
                        names='location',
                        title="Top 10 Locations"
                    )
                    st.plotly_chart(fig3, use_container_width=True)
            
            with tab3:
                st.subheader("Team Information")
                
                # Get unique teams from filtered contacts
                team_ids = filtered_contacts['team_id'].unique()
                teams_data = self.teams_df[self.teams_df['team_id'].isin(team_ids)]
                
                # Search functionality for teams
                team_search = st.text_input("Search teams (name, school, club)", key="team_search")
                if team_search:
                    search_mask = (
                        teams_data['team_name'].str.contains(team_search, case=False, na=False) |
                        teams_data['school_name'].str.contains(team_search, case=False, na=False)
                    )
                    teams_data = teams_data[search_mask]
                
                # Display teams in a table
                team_display_cols = [
                    'team_name', 'school_name', 'gender', 
                    'draw_name', 'url'
                ]
                
                st.dataframe(
                    teams_data[team_display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("Team URL"),
                        "team_name": st.column_config.TextColumn("Team Name", width="medium"),
                        "school_name": st.column_config.TextColumn("School", width="medium"),
                        "gender": st.column_config.TextColumn("Gender", width="small"),
                        "draw_name": st.column_config.TextColumn("Draw/Competition", width="medium")
                    }
                )
                
                # Team gender distribution
                gender_counts = teams_data['gender'].value_counts().reset_index()
                gender_counts.columns = ['gender', 'count']
                
                if not gender_counts.empty:
                    fig4 = px.pie(
                        gender_counts,
                        values='count',
                        names='gender',
                        title="Team Gender Distribution"
                    )
                    st.plotly_chart(fig4, use_container_width=True)
            
            with tab4:
                st.subheader("Match Schedule")
                
                # Get matches related to filtered teams
                team_ids = filtered_contacts['team_id'].unique()
                matches_data = self.matches_df[
                    (self.matches_df['home_team_id'].isin(team_ids)) | 
                    (self.matches_df['away_team_id'].isin(team_ids))
                ]
                
                # Search functionality for matches
                match_search = st.text_input("Search matches (team names)", key="match_search")
                if match_search:
                    search_mask = (
                        matches_data['home_team_name'].str.contains(match_search, case=False, na=False) |
                        matches_data['away_team_name'].str.contains(match_search, case=False, na=False)
                    )
                    matches_data = matches_data[search_mask]
                
                # Display matches in a table
                match_display_cols = [
                    'match_date', 'match_time', 'home_team_name', 
                    'away_team_name', 'score', 'status', 'url'
                ]
                
                st.dataframe(
                    matches_data[match_display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("Match URL"),
                        "match_date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                        "match_time": st.column_config.TextColumn("Time", width="small"),
                        "home_team_name": st.column_config.TextColumn("Home Team", width="medium"),
                        "away_team_name": st.column_config.TextColumn("Away Team", width="medium"),
                        "score": st.column_config.TextColumn("Score", width="small"),
                        "status": st.column_config.TextColumn("Status", width="small")
                    }
                )
                
                # Export functionality
                if st.button("Export Filtered Matches to CSV"):
                    csv = matches_data[match_display_cols].to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="lta_matches.csv",
                        mime="text/csv"
                    )
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.error("Please make sure the database exists and contains the required tables.")
        finally:
            self.cleanup()

if __name__ == "__main__":
    dashboard = LTADashboard()
    dashboard.run() 