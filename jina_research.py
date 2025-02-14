import os
import requests
import json
from typing import Dict, Optional, Union
from dotenv import load_dotenv

class JinaDeepResearch:
    """Client for interacting with Jina DeepResearch API"""
    
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        
        # Get API keys from environment variables
        self.jina_api_key = os.getenv('JINA_API_KEY')
        if not self.jina_api_key:
            raise ValueError("JINA_API_KEY environment variable is required")
            
        self.base_url = "https://deepsearch.jina.ai/v1/chat/completions"
    
    def search_email(self, person_info: Dict[str, str]) -> Dict[str, Union[str, None]]:
        """
        Search for email address of a specific person using their LinkedIn information
        
        Args:
            person_info: Dictionary containing person details like:
                - full_name: Full name of the person
                - company: Current company
                - title: Current job title
                - linkedin_url: LinkedIn profile URL
                
        Returns:
            Dict containing:
                - email: Found email address or None
                - confidence: Confidence score (high/medium/low)
                - source: Source of the email if found
                - raw_response: Full API response
        """
        # Construct targeted search query
        query = f"""Find the work email address for this person:
        Name: {person_info.get('full_name', '')}
        Company: {person_info.get('company', '')}
        Title: {person_info.get('title', '')}
        LinkedIn: {person_info.get('linkedin_url', '')}
        
        Please return only their most likely current work email address with confidence level and source."""
        
        response = self.query(query)
        
        # Parse response to extract email
        try:
            content = response['choices'][0]['message']['content']
            
            # Initialize result
            result = {
                'email': None,
                'confidence': 'low',
                'source': None,
                'raw_response': content
            }
            
            # Basic email extraction - you may want to enhance this parsing
            if '@' in content:
                # Extract first email-like string
                import re
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                if email_match:
                    result['email'] = email_match.group(0)
                
                # Determine confidence
                if 'high confidence' in content.lower():
                    result['confidence'] = 'high'
                elif 'medium confidence' in content.lower():
                    result['confidence'] = 'medium'
                
                # Try to extract source
                if 'source:' in content.lower():
                    source_match = re.search(r'source:(.+?)(?:\n|$)', content, re.I)
                    if source_match:
                        result['source'] = source_match.group(1).strip()
            
            return result
            
        except (KeyError, IndexError) as e:
            print(f"Error parsing response: {e}")
            return {
                'email': None,
                'confidence': 'low',
                'source': None,
                'raw_response': str(response)
            }

    def query(self, question: str, max_budget: int = 1000000, max_bad_attempts: int = 3) -> Dict:
        """
        Send a query to Jina DeepResearch
        
        Args:
            question: The question to research
            max_budget: Maximum token budget (default 1M)
            max_bad_attempts: Maximum failed attempts before giving up
            
        Returns:
            Dict containing the response data
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.jina_api_key}"
        }
        
        data = {
            "model": "jina-deepsearch-v1",
            "messages": [{"role": "user", "content": question}],
            "max_budget": max_budget,
            "max_bad_attempts": max_bad_attempts
        }
        
        try:
            response = requests.post(
                self.base_url,
                headers=headers,
                json=data
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            raise

def main():
    """Example usage"""
    try:
        # Initialize client
        client = JinaDeepResearch()
        
        # Example person search
        person_info = {
            'full_name': 'John Smith',
            'company': 'Acme Corp',
            'title': 'Senior Software Engineer',
            'linkedin_url': 'https://linkedin.com/in/johnsmith'
        }
        
        result = client.search_email(person_info)
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 