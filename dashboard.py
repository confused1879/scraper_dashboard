import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
from cryptography.fernet import Fernet
import tempfile
import os
import itertools
import pyperclip
import dns.resolver
import smtplib
import re
from email_validator import validate_email, EmailNotValidError
from concurrent.futures import ThreadPoolExecutor
import time
import requests
from bs4 import BeautifulSoup
from html import unescape
from urllib.parse import urlparse
from jina_research import JinaDeepResearch
from lta_dashboard import LTADashboard

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
        cursor = conn.cursor()
        
        try:
            # First check if linkedin_data table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='linkedin_data';
            """)
            linkedin_table_exists = cursor.fetchone() is not None
            
            if linkedin_table_exists:
                # Use the original query with linkedin_data
                query = """
                    SELECT 
                        p.*,
                        s.name as school_name,
                        ld.base_url as domain_name
                    FROM profiles p
                    JOIN schools s ON p.school_id = s.id
                    LEFT JOIN linkedin_data ld ON ld.school = s.name
                """
            else:
                # Fallback query without linkedin_data
                query = """
                    SELECT 
                        p.*,
                        s.name as school_name,
                        NULL as domain_name
                    FROM profiles p
                    JOIN schools s ON p.school_id = s.id
                """
            
            self.df = pd.read_sql_query(query, conn)
            
            # Get unique values for filters
            self.schools = sorted(self.df['company'].unique())
            self.titles = sorted(self.df['title'].dropna().unique())
            self.locations = sorted(self.df['location'].dropna().unique())
            
        finally:
            conn.close()

    def verify_email_syntax(self, email):
        """Verify email syntax using email-validator library."""
        st.write(f"🔍 Checking syntax for: {email}")
        try:
            validate_email(email)
            st.write(f"✓ Syntax valid for: {email}")
            return True
        except EmailNotValidError as e:
            st.write(f"✗ Syntax invalid for {email}: {str(e)}")
            return False

    def verify_mx_record(self, domain):
        """Verify if domain has valid MX records."""
        st.write(f"🔍 Checking MX records for domain: {domain}")
        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            result = len(mx_records) > 0
            if result:
                st.write(f"✓ Found MX records for: {domain}")
            else:
                st.write(f"✗ No MX records found for: {domain}")
            return result
        except Exception as e:
            st.write(f"✗ Error checking MX records for {domain}: {str(e)}")
            return False

    def verify_smtp(self, email, domain):
        """Verify email existence using SMTP handshake."""
        st.write(f"🔍 Attempting SMTP verification for: {email}")
        try:
            # Get MX record
            st.write(f"  Getting MX records for {domain}...")
            mx_records = dns.resolver.resolve(domain, 'MX')
            mx_host = str(mx_records[0].exchange)
            st.write(f"  Using MX host: {mx_host}")

            # Connect to SMTP server
            st.write(f"  Connecting to SMTP server...")
            smtp = smtplib.SMTP(timeout=10)
            smtp.connect(mx_host)
            
            # Set debug level
            smtp.set_debuglevel(1)
            
            st.write(f"  Connected, sending HELO...")
            smtp.ehlo('gmail.com')  # Using a more common domain
            
            st.write(f"  Sending MAIL FROM...")
            smtp.mail('verify@gmail.com')  # Using a more common email
            
            st.write(f"  Sending RCPT TO...")
            code, message = smtp.rcpt(email)
            smtp.quit()
            
            st.write(f"  SMTP response code: {code}")
            st.write(f"  SMTP response message: {message}")
            
            # Many servers will return 250 (success) or 550/553 (invalid)
            # But some servers might return other codes or always accept
            if code == 250:
                st.write(f"✓ Email likely exists: {email}")
                return True
            elif code in [550, 553]:
                st.write(f"✗ Email likely doesn't exist: {email}")
                return False
            else:
                st.write(f"? Inconclusive result for {email} (code {code})")
                return "Inconclusive"
            
        except Exception as e:
            st.write(f"✗ SMTP verification failed for {email}: {str(e)}")
            return "Error"

    def verify_email(self, email):
        """Complete email verification process."""
        st.write(f"\n📧 Starting verification for: {email}")
        start_time = time.time()
        
        results = {
            'syntax': False,
            'mx_record': False,
            'smtp': 'Not attempted'
        }
        
        # Check syntax
        results['syntax'] = self.verify_email_syntax(email)
        if not results['syntax']:
            st.write(f"⏱️ Syntax check took: {time.time() - start_time:.2f}s")
            return results

        # Get domain from email
        domain = email.split('@')[1]
        
        # Check MX records
        mx_start_time = time.time()
        results['mx_record'] = self.verify_mx_record(domain)
        st.write(f"⏱️ MX check took: {time.time() - mx_start_time:.2f}s")
        if not results['mx_record']:
            return results

        # Attempt SMTP verification
        smtp_start_time = time.time()
        results['smtp'] = self.verify_smtp(email, domain)
        st.write(f"⏱️ SMTP check took: {time.time() - smtp_start_time:.2f}s")
        
        total_time = time.time() - start_time
        st.write(f"⏱️ Total verification time for {email}: {total_time:.2f}s\n")
        return results

    def verify_email_kickbox(self, email):
        """Verify email using Kickbox API."""
        st.write(f"🔍 Verifying with Kickbox API: {email}")
        start_time = time.time()
        
        try:
            if 'kickbox_api_key' not in st.secrets:
                st.error("Kickbox API key not found in secrets")
                return None
                
            api_key = st.secrets['kickbox_api_key']
            url = f"https://api.kickbox.com/v2/verify"
            
            params = {
                'email': email,
                'apikey': api_key
            }
            
            response = requests.get(url, params=params)
            result = response.json()
            
            st.write(f"⏱️ Kickbox verification took: {time.time() - start_time:.2f}s")
            
            # Log detailed response for debugging
            st.write("Kickbox Response:", result)
            
            return {
                'result': result.get('result'),  # deliverable, undeliverable, risky, unknown
                'reason': result.get('reason'),
                'did_you_mean': result.get('did_you_mean'),
                'success': result.get('success', False),
                'disposable': result.get('disposable', False),
                'accept_all': result.get('accept_all', False),
                'role': result.get('role', False),
                'free': result.get('free', False),
                'score': result.get('sendex', 0)
            }
            
        except Exception as e:
            st.write(f"✗ Kickbox verification failed: {str(e)}")
            return None

    def extract_emails_from_url(self, url, target_email):
        """Extract emails from a specific URL."""
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Remove scripts/styles
                for tag in soup(["script", "style"]):
                    tag.decompose()
                
                # Check mailto: links
                mailto_matches = []
                for a_tag in soup.find_all('a', href=True):
                    if a_tag['href'].lower().startswith("mailto:"):
                        found_email = a_tag['href'].lower().replace("mailto:", "").strip()
                        if found_email == target_email.lower():
                            mailto_matches.append({
                                'context': f"Found in mailto link: {found_email}",
                                'url': url,
                                'type': 'mailto'
                            })
                
                # Get text content and normalize
                text = unescape(soup.get_text())
                return mailto_matches
            return []
        except Exception as e:
            st.write(f"Error fetching {url}: {e}")
            return []

    def check_for_email_variants(self, text, email, first_name, last_name):
        """Check for various email obfuscation patterns."""
        local, domain = email.split("@")
        domain_part, _, tld = domain.rpartition('.')
        
        patterns = [
            rf"{re.escape(email)}",  # Exact match
            rf"{re.escape(local)}\s*(?:@|\[at\]|\(at\))\s*{re.escape(domain)}",  # Basic obfuscation
            rf"{re.escape(local)}\s*(?:@|\[at\]|\(at\))\s*{re.escape(domain_part)}\s*(?:\.|\[dot\]|\(dot\))\s*{re.escape(tld)}",  # Full obfuscation
            rf"{re.escape(first_name)}\.{re.escape(last_name)}\s*(?:@|\[at\]|\(at\))\s*{re.escape(domain)}",  # Name-based pattern
            rf"{re.escape(first_name[0])}{re.escape(last_name)}\s*(?:@|\[at\]|\(at\))\s*{re.escape(domain)}"  # Initial-based pattern
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return {
                    'pattern': pattern,
                    'matched_text': match.group(0)
                }
        return None

    def verify_email_brightdata(self, first_name, last_name, domain, email, title):
        """Verify email using BrightData SERP API with two-step verification."""
        st.write(f"🔍 Starting BrightData search for: {email}")
        start_time = time.time()
        
        try:
            if 'brightdata_api_key' not in st.secrets:
                st.error("BrightData API key not found in secrets")
                return None
            
            api_key = st.secrets['brightdata_api_key']
            url = "https://api.brightdata.com/request"
            
            # Try two different search queries
            queries = [
                f'"{email}"',  # First try exact email
                f'"{first_name} {last_name}" "{email}"'  # Then try name with email
            ]
            
            exact_matches = []
            
            for query in queries:
                st.write(f"\nTrying query: {query}")
                
                search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
                
                payload = {
                    "zone": "serp_api1",
                    "url": search_url,
                    "format": "raw",
                    "method": "GET",
                    "country": "US"
                }
                
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                response = requests.request("POST", url, json=payload, headers=headers)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Remove script and style elements
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    # Get search results
                    search_results = soup.find_all('div', class_='g')  # Google search result containers
                    
                    if not search_results:
                        st.write("No search results found for this query")
                        continue
                    
                    for result in search_results:
                        # Get text content
                        result_text = result.get_text().lower()
                        
                        # Skip if this is a "no results found" message
                        if any(phrase in result_text for phrase in [
                            "no results found for",
                            "did you mean",
                            "search instead for",
                            "showing results for"
                        ]):
                            continue
                        
                        # Look for exact match of the email
                        if email.lower() in result_text:
                            # Get the title and URL if available
                            result_title = result.find('h3')
                            result_link = result.find('a')
                            
                            # Get context around the match
                            start_idx = result_text.find(email.lower())
                            context = result_text[max(0, start_idx-50):min(len(result_text), start_idx+len(email)+50)]
                            
                            match_info = {
                                'email': email,
                                'context': context.strip(),
                                'search_url': search_url,
                                'result_title': result_title.get_text() if result_title else None,
                                'result_url': result_link['href'] if result_link else None,
                                'query_used': query
                            }
                            
                            if match_info not in exact_matches:  # Avoid duplicates
                                exact_matches.append(match_info)
                                st.write(f"Found match in: {match_info['result_title'] if match_info['result_title'] else 'Untitled'}")
                    
                    if exact_matches:
                        break  # Stop if we found matches
                
                else:
                    st.write(f"Search failed for query: {query}")
                    st.write(f"Error: {response.text}")
            
            search_time = time.time() - start_time
            st.write(f"\n⏱️ Search completed in {search_time:.2f}s")
            
            if exact_matches:
                st.write(f"✓ Found {len(exact_matches)} matches for: {email}")
            else:
                st.write(f"✗ No matches found for: {email}")
            
            return {
                'exact_match': len(exact_matches) > 0,
                'exact_match_details': exact_matches,
                'search_url': search_url
            }
            
        except Exception as e:
            st.write(f"✗ BrightData search failed: {str(e)}")
            return None

    def generate_email_permutations(self, first_name, last_name, domain, use_kickbox=False, use_brightdata=False):
        """Generate possible email patterns with verification."""
        if not first_name or not last_name or not domain:
            return []
            
        # Clean domain by removing www. if present
        domain = domain.replace('www.', '')
        
        # Updated patterns list with most common patterns first
        patterns = [
            "{first}{last}@{domain}",
            "{f}{last}@{domain}",
            "{first}.{last}@{domain}",
            "{first}_{last}@{domain}",
            "{last}{first}@{domain}",
            "{first}{l}@{domain}",
            "{f}.{last}@{domain}",
            "{last}.{first}@{domain}",
            "{l}{first}@{domain}",
            "{last}{f}@{domain}",
            "{first}.{l}@{domain}",
            "{last}.{f}@{domain}"
        ]
        
        # Lowercase everything
        first_lower = first_name.lower()
        last_lower = last_name.lower()
        f = first_lower[0]
        l = last_lower[0]

        # Generate email patterns
        email_patterns = [pattern.format(
            first=first_lower,
            last=last_lower,
            f=f,
            l=l,
            domain=domain
        ) for pattern in patterns]

        # Verify each email
        verified_patterns = []
        
        if use_brightdata:
            # Use BrightData SERP API for verification
            for email in email_patterns:
                brightdata_result = self.verify_email_brightdata(
                    first_name, last_name, domain, email, ""
                )
                verified_patterns.append({
                    'email': email,
                    'verification': {
                        'brightdata': brightdata_result
                    }
                })
        elif use_kickbox:
            # Use Kickbox API for verification
            for email in email_patterns:
                kickbox_result = self.verify_email_kickbox(email)
                verified_patterns.append({
                    'email': email,
                    'verification': {
                        'kickbox': kickbox_result
                    }
                })
        else:
            # Use existing SMTP verification
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(self.verify_email, email): email 
                          for email in email_patterns}
                
                with st.spinner('Verifying email patterns...'):
                    for future in futures:
                        email = futures[future]
                        try:
                            results = future.result()
                            verified_patterns.append({
                                'email': email,
                                'verification': results
                            })
                        except Exception as e:
                            verified_patterns.append({
                                'email': email,
                                'verification': {'error': str(e)}
                            })

        return verified_patterns

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
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "Profile Distribution", 
                "Job Titles", 
                "Locations",
                "DeepSearch",
                "Batch DeepSearch",
                "LTA Schools Dashboard"  # New tab
            ])
            
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
            
            with tab4:
                st.subheader("Jina DeepSearch Email")
                
                # Add search functionality for this tab
                search_term = st.text_input("Search profiles (name, title, company)", key="deep_search_filter")
                if search_term:
                    search_mask = (
                        filtered_df['name'].str.contains(search_term, case=False, na=False) |
                        filtered_df['title'].str.contains(search_term, case=False, na=False) |
                        filtered_df['company'].str.contains(search_term, case=False, na=False)
                    )
                    search_df = filtered_df[search_mask]
                else:
                    search_df = filtered_df
                
                # Create selection interface using the filtered data
                selected_row_index = st.selectbox(
                    "Select Profile",
                    range(len(search_df)),
                    format_func=lambda x: f"{search_df.iloc[x]['name']} - {search_df.iloc[x]['title']} at {search_df.iloc[x]['company']}",
                    key="profile_selector"
                )

                if selected_row_index is not None:
                    selected_row = search_df.iloc[selected_row_index]
                    
                    st.write("### Selected Profile")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Name:** {selected_row['name']}")
                        st.write(f"**Title:** {selected_row['title']}")
                        st.write(f"**Company:** {selected_row['company']}")
                    
                    with col2:
                        st.write(f"**Location:** {selected_row['location']}")
                        st.write(f"**School:** {selected_row['school_name']}")
                        if pd.notna(selected_row['domain_name']):
                            st.write(f"**Domain:** {selected_row['domain_name']}")
                    
                    if st.button("Search with DeepResearch", key="deep_search_button"):
                        with st.spinner("Searching with Jina DeepResearch..."):
                            try:
                                jina_client = JinaDeepResearch()
                                result = jina_client.search_email({
                                    'full_name': selected_row['name'],
                                    'company': selected_row['company'],
                                    'title': selected_row['title'],
                                    'linkedin_url': selected_row['profile_url'] if pd.notna(selected_row['profile_url']) else ''
                                })
                                
                                # Display results in an organized way
                                if result['email']:
                                    st.success(f"Found email: {result['email']}")
                                    
                                    # Create metrics for confidence and source
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.metric("Confidence", result['confidence'])
                                    with col2:
                                        if result['source']:
                                            st.metric("Source", result['source'])
                                    
                                    # Display thoughts if available
                                    if result['thoughts']:
                                        with st.expander("Show Search Process"):
                                            st.markdown("**Search Reasoning:**")
                                            st.write(result['thoughts'])
                                    
                                    # Copy button for the email
                                    if st.button("📋 Copy Email", key="copy_email_button"):
                                        pyperclip.copy(result['email'])
                                        st.info("Email copied to clipboard!")
                                else:
                                    st.warning("No email found")
                                
                                # Show raw response in expandable section
                                with st.expander("Show Raw Response"):
                                    st.code(result['raw_response'])
                                    
                            except Exception as e:
                                st.error(f"Error during search: {str(e)}")
                                st.error("Please check your Jina API key and try again.")
            with tab5:
                st.subheader("Batch Jina DeepSearch")
                
                # Initialize session state for storing selected profiles
                if 'selected_profiles' not in st.session_state:
                    st.session_state.selected_profiles = []
                if 'search_results' not in st.session_state:
                    st.session_state.search_results = []

                # Search and select profiles
                search_term = st.text_input("Search profiles (name, title, company)", key="batch_search_filter")
                if search_term:
                    search_mask = (
                        filtered_df['name'].str.contains(search_term, case=False, na=False) |
                        filtered_df['title'].str.contains(search_term, case=False, na=False) |
                        filtered_df['company'].str.contains(search_term, case=False, na=False)
                    )
                    search_df = filtered_df[search_mask]
                else:
                    search_df = filtered_df

                # Display profiles for selection
                selected_row_index = st.selectbox(
                    "Select Profile to Add to Batch",
                    range(len(search_df)),
                    format_func=lambda x: f"{search_df.iloc[x]['name']} - {search_df.iloc[x]['title']} at {search_df.iloc[x]['company']}",
                    key="batch_profile_selector"
                )

                # Add to batch button
                if st.button("Add to Batch", key="add_to_batch"):
                    selected_row = search_df.iloc[selected_row_index]
                    profile_info = {
                        'name': selected_row['name'],
                        'title': selected_row['title'],
                        'company': selected_row['company'],
                        'school': selected_row['school_name'],
                        'profile_url': selected_row['profile_url'] if pd.notna(selected_row['profile_url']) else '',
                        'domain': selected_row['domain_name'] if pd.notna(selected_row['domain_name']) else ''
                    }
                    
                    # Check if profile is already in batch
                    if not any(p['name'] == profile_info['name'] for p in st.session_state.selected_profiles):
                        st.session_state.selected_profiles.append(profile_info)
                        st.success(f"Added {profile_info['name']} to batch")
                    else:
                        st.warning("Profile already in batch")

                # Display selected profiles
                if st.session_state.selected_profiles:
                    st.write("### Selected Profiles")
                    
                    # Create a DataFrame of selected profiles for display
                    selected_df = pd.DataFrame(st.session_state.selected_profiles)
                    st.dataframe(
                        selected_df,
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Clear batch button
                    if st.button("Clear Batch", key="clear_batch"):
                        st.session_state.selected_profiles = []
                        st.session_state.search_results = []
                        st.success("Batch cleared")

                    # Search batch button
                    if st.button("Search Batch with DeepResearch", key="search_batch"):
                        st.session_state.search_results = []  # Clear previous results
                        
                        progress_bar = st.progress(0)
                        for idx, profile in enumerate(st.session_state.selected_profiles):
                            with st.spinner(f"Searching {profile['name']}..."):
                                try:
                                    jina_client = JinaDeepResearch()
                                    result = jina_client.search_email({
                                        'full_name': profile['name'],
                                        'company': profile['company'],
                                        'title': profile['title'],
                                        'linkedin_url': profile['profile_url']
                                    })
                                    
                                    # Add search result to profile info
                                    search_result = {
                                        **profile,
                                        'email': result['email'] if result['email'] else 'Not found',
                                        'confidence': result['confidence'],
                                        'source': result['source'] if result['source'] else 'N/A',
                                        'thoughts': result['thoughts'] if result['thoughts'] else 'N/A'
                                    }
                                    st.session_state.search_results.append(search_result)
                                    
                                except Exception as e:
                                    st.error(f"Error searching {profile['name']}: {str(e)}")
                                    search_result = {
                                        **profile,
                                        'email': 'Error',
                                        'confidence': 'N/A',
                                        'source': str(e),
                                        'thoughts': 'N/A'
                                    }
                                    st.session_state.search_results.append(search_result)
                                
                                # Update progress
                                progress_bar.progress((idx + 1) / len(st.session_state.selected_profiles))
                        
                        st.success("Batch search completed!")

                # Display and export results
                if st.session_state.search_results:
                    st.write("### Search Results")
                    
                    # Create DataFrame from results
                    results_df = pd.DataFrame(st.session_state.search_results)
                    
                    # Display results with thoughts in expandable sections
                    for idx, row in results_df.iterrows():
                        with st.expander(f"{row['name']} - {row['email']}"):
                            st.write(f"**Confidence:** {row['confidence']}")
                            st.write(f"**Source:** {row['source']}")
                            if row['thoughts'] != 'N/A':
                                st.markdown("**Search Process:**")
                                st.write(row['thoughts'])
                    
                    # Export to CSV with thoughts included
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="Download Results CSV",
                        data=csv,
                        file_name="jina_deepsearch_results.csv",
                        mime="text/csv",
                        key="download_results"
                    )

            with tab6:
                st.subheader("LTA Schools Dashboard")
                st.markdown("---")
                st.markdown("### LTA Tennis Schools Data Explorer")
                st.markdown("Filter and analyze LTA school and contact information")
                
                # Initialize LTA Dashboard
                lta_dashboard = LTADashboard()
                
                # Check if database exists
                if not os.path.exists(lta_dashboard.db_path):
                    st.error(f"Database file {lta_dashboard.db_path} not found. Please run the lta_db_loader.py script first.")
                else:
                    try:
                        # Load LTA data with a custom query to fix the team name issue
                        conn = lta_dashboard.get_connection()
                        try:
                            # Load clubs data
                            lta_dashboard.clubs_df = pd.read_sql_query("""
                                SELECT * FROM clubs
                            """, conn)
                            
                            # Load teams data
                            lta_dashboard.teams_df = pd.read_sql_query("""
                                SELECT * FROM teams
                            """, conn)
                            
                            # Load contacts data with team and club information - fixed query
                            lta_dashboard.contacts_df = pd.read_sql_query("""
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
                            
                            # Debug query to check raw team data
                            raw_teams_df = pd.read_sql_query("""
                                SELECT * FROM teams LIMIT 10
                            """, conn)
                            
                            # Fix team_name if it's None or NaN
                            def fix_team_name(row):
                                if pd.isna(row['team_name']) or row['team_name'] is None:
                                    # Try to construct a team name from school_name and gender
                                    school = row['school_name'] if pd.notna(row['school_name']) else ''
                                    gender = row['gender'] if pd.notna(row['gender']) else ''
                                    if school and gender:
                                        return f"{school} {gender}"
                                    elif school:
                                        return school
                                    else:
                                        return "Unknown Team"
                                return row['team_name']
                            
                            # Apply the fix to the contacts dataframe
                            lta_dashboard.contacts_df['team_name'] = lta_dashboard.contacts_df.apply(fix_team_name, axis=1)
                            
                            # Load matches data
                            lta_dashboard.matches_df = pd.read_sql_query("""
                                SELECT * FROM matches
                            """, conn)
                            
                            # Get unique values for filters
                            lta_dashboard.club_names = sorted(lta_dashboard.clubs_df['club_name'].dropna().unique())
                            lta_dashboard.school_names = sorted(lta_dashboard.teams_df['school_name'].dropna().unique())
                            lta_dashboard.locations = sorted(pd.concat([
                                lta_dashboard.clubs_df['location'].dropna(),
                                lta_dashboard.contacts_df['location'].dropna()
                            ]).unique())
                            lta_dashboard.roles = sorted(lta_dashboard.contacts_df['role'].dropna().unique())
                            lta_dashboard.genders = sorted(lta_dashboard.teams_df['gender'].dropna().unique())
                            
                        finally:
                            conn.close()
                        
                        # Sidebar filters for LTA data
                        st.sidebar.markdown("---")
                        st.sidebar.header("LTA Filters")
                        
                        # School/Club filter
                        filter_type = st.sidebar.radio(
                            "Filter LTA by:",
                            ["All", "School", "Club"],
                            key="lta_filter_type"
                        )
                        
                        if filter_type == "School":
                            selected_schools = st.sidebar.multiselect(
                                "Select LTA Schools",
                                lta_dashboard.school_names,
                                default=[],
                                key="lta_schools"
                            )
                        elif filter_type == "Club":
                            selected_clubs = st.sidebar.multiselect(
                                "Select LTA Clubs",
                                lta_dashboard.club_names,
                                default=[],
                                key="lta_clubs"
                            )
                        
                        # Location filter
                        selected_locations = st.sidebar.multiselect(
                            "Select LTA Locations",
                            lta_dashboard.locations,
                            default=[],
                            key="lta_locations"
                        )
                        
                        # Role filter
                        selected_roles = st.sidebar.multiselect(
                            "Select LTA Roles",
                            lta_dashboard.roles,
                            default=[],
                            key="lta_roles"
                        )
                        
                        # Gender filter
                        selected_genders = st.sidebar.multiselect(
                            "Select LTA Team Gender",
                            lta_dashboard.genders,
                            default=[],
                            key="lta_genders"
                        )
                        
                        # Apply filters to contacts dataframe
                        filtered_contacts = lta_dashboard.contacts_df.copy()
                        
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
                            st.metric("Total LTA Contacts", len(filtered_contacts))
                        with col2:
                            st.metric("LTA Schools/Clubs", len(filtered_contacts[['school_name', 'club_name']].drop_duplicates()))
                        with col3:
                            st.metric("LTA Teams", len(filtered_contacts['team_id'].unique()))
                        with col4:
                            st.metric("LTA Locations", len(filtered_contacts['location'].dropna().unique()))
                        
                        # Create tabs for different LTA views
                        st.markdown("### LTA Data Views")
                        lta_tab1, lta_tab2, lta_tab3, lta_tab4 = st.tabs([
                            "LTA Contact List", 
                            "LTA School/Club Distribution", 
                            "LTA Team Information",
                            "LTA Match Schedule"
                        ])
                        
                        with lta_tab1:
                            st.subheader("Contact List")
                            
                            # Debug information to help identify issues
                            with st.expander("Debug Information"):
                                st.write("### Data Sample")
                                st.write("This shows a sample of the raw data to help identify any issues:")
                                st.dataframe(
                                    filtered_contacts.head(5),
                                    use_container_width=True
                                )
                                
                                # Show raw team data
                                st.write("### Raw Team Data")
                                st.write("This shows the raw data from the teams table:")
                                st.dataframe(
                                    raw_teams_df,
                                    use_container_width=True
                                )
                                
                                # Show column names and types - Fixed to prevent type conversion errors
                                st.write("### Column Information")
                                col_info = pd.DataFrame({
                                    'Column': filtered_contacts.columns,
                                    'Type': [str(dtype) for dtype in filtered_contacts.dtypes]
                                })
                                
                                # Add sample values safely
                                sample_values = []
                                for col in filtered_contacts.columns:
                                    try:
                                        values = filtered_contacts[col].dropna().head(3).tolist()
                                        sample_values.append(str(values) if values else "[]")
                                    except Exception as e:
                                        sample_values.append(f"Error: {str(e)}")
                                
                                col_info['Sample Values'] = sample_values
                                st.dataframe(col_info, use_container_width=True)
                            
                            # Search functionality
                            search_term = st.text_input("Search contacts (name, email, school, club)", key="lta_contact_search")
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
                                'name', 'email', 'phone', 'school_name', 'gender'
                            ]
                            
                            # Remove duplicate contacts (same person might be associated with multiple teams)
                            deduplicated_contacts = filtered_contacts.drop_duplicates(subset=['contact_id'])
                            
                            # Ensure all columns are of the correct type for display
                            display_df = deduplicated_contacts[display_cols].copy()
                            
                            # Convert all columns to string for safe display
                            for col in display_df.columns:
                                display_df[col] = display_df[col].astype(str).replace('nan', '')
                            
                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "email": st.column_config.LinkColumn("Email", width="medium"),
                                    "name": st.column_config.TextColumn("Contact Name", width="medium"),
                                    "phone": st.column_config.TextColumn("Phone", width="medium"),
                                    "school_name": st.column_config.TextColumn("School", width="medium"),
                                    "gender": st.column_config.TextColumn("Gender", width="small")
                                }
                            )
                            
                            # Export functionality
                            if st.button("Export Filtered Contacts to CSV", key="lta_export_contacts"):
                                # Use the display_df for export to ensure consistent data
                                csv = display_df.to_csv(index=False)
                                st.download_button(
                                    label="Download CSV",
                                    data=csv,
                                    file_name="lta_contacts.csv",
                                    mime="text/csv",
                                    key="lta_download_contacts"
                                )
                        
                        with lta_tab2:
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
                        
                        with lta_tab3:
                            st.subheader("Team Information")
                            
                            # Get unique teams from filtered contacts
                            team_ids = filtered_contacts['team_id'].unique()
                            teams_data = lta_dashboard.teams_df[lta_dashboard.teams_df['team_id'].isin(team_ids)]
                            
                            # Search functionality for teams
                            team_search = st.text_input("Search teams (name, school, club)", key="lta_team_search")
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
                        
                        with lta_tab4:
                            st.subheader("Match Schedule")
                            
                            # Get matches related to filtered teams
                            team_ids = filtered_contacts['team_id'].unique()
                            matches_data = lta_dashboard.matches_df[
                                (lta_dashboard.matches_df['home_team_id'].isin(team_ids)) | 
                                (lta_dashboard.matches_df['away_team_id'].isin(team_ids))
                            ]
                            
                            # Search functionality for matches
                            match_search = st.text_input("Search matches (team names)", key="lta_match_search")
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
                            if st.button("Export Filtered Matches to CSV", key="lta_export_matches"):
                                csv = matches_data[match_display_cols].to_csv(index=False)
                                st.download_button(
                                    label="Download CSV",
                                    data=csv,
                                    file_name="lta_matches.csv",
                                    mime="text/csv",
                                    key="lta_download_matches"
                                )
                    except Exception as e:
                        st.error(f"Error loading LTA data: {str(e)}")
                        st.error("Please make sure the LTA database exists and contains the required tables.")

            # Data table
            st.subheader("Profile Data")
            
            # Updated display columns to include domain_name
            display_cols = ['name', 'title', 'company', 'location', 'school_name', 
                           'domain_name', 'duration', 'connection_degree', 
                           'mutual_connections', 'profile_url', 'about']
            
            # Search functionality
            search_term = st.text_input("Search profiles (name, title, company, or about)")
            if search_term:
                search_mask = (
                    filtered_df['name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['title'].str.contains(search_term, case=False, na=False) |
                    filtered_df['company'].str.contains(search_term, case=False, na=False) |
                    filtered_df['about'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[search_mask]
            
            # Data table with updated column configuration
            st.dataframe(
                filtered_df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "profile_url": st.column_config.LinkColumn("Profile URL"),
                    "domain_name": st.column_config.TextColumn("Domain Name"),
                    "about": st.column_config.TextColumn("About", width="large")
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
                
            # Email search section
            def email_search_section():
                st.subheader("Email Search")
                
                # Get selected person's data
                selected_person = st.selectbox(
                    "Select Person",
                    filtered_df,  # Replace with your actual data source
                    format_func=lambda x: f"{x['name']} - {x['title']} at {x['company']}"  # Adjust based on your data structure
                )
                
                if st.button("Search Email"):
                    with st.spinner("Searching for email..."):
                        jina_client = JinaDeepResearch()
                        result = jina_client.search_email({
                            'full_name': selected_person['name'],
                            'company': selected_person['company'],
                            'title': selected_person['title'],
                            'linkedin_url': selected_person['profile_url']
                        })
                        
                        if result['email']:
                            st.success(f"Found email: {result['email']}")
                            st.info(f"Confidence: {result['confidence']}")
                            if result['source']:
                                st.info(f"Source: {result['source']}")
                        else:
                            st.warning("No email found")
                        
                        with st.expander("Raw Response"):
                            st.text(result['raw_response'])

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.error("Please make sure the database exists and contains the required tables.")
        finally:
            self.cleanup()

if __name__ == "__main__":
    dashboard = LinkedInDashboard()
    dashboard.run() 