import os
import requests
import json
from typing import Dict, Optional, Union
from dotenv import load_dotenv
import streamlit as st
import re

class JinaDeepResearch:
    """Client for interacting with Jina DeepResearch API"""
    
    def __init__(self):
        # Get API key from Streamlit secrets instead of env
        if 'JINA_API_KEY' not in st.secrets:
            raise ValueError("JINA_API_KEY not found in Streamlit secrets")
            
        self.jina_api_key = st.secrets['JINA_API_KEY']
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
                - thoughts: Extracted thought process
                - raw_response: Full API response
        """
        query = f"""Find the current work email address for this person:
        Name: {person_info.get('full_name', '')}
        Current Company: {person_info.get('company', '')}
        Current Title: {person_info.get('title', '')}
        LinkedIn Profile: {person_info.get('linkedin_url', '')}

        Instructions:
        1. Search for their most current work email address
        2. Focus on official company sources, press releases, or verified business listings
        3. Check for email patterns used at their current company
        4. Verify any found email against company domain records
        5. Assess confidence level (high/medium/low) based on source reliability
        6. Include source of information and your reasoning process

        Please format response with:
        <think>Your step-by-step reasoning process</think>
        Email: [found_email]
        Confidence: [high/medium/low]
        Source: [where the email was found]
        """
        
        response = self.query(query)
        
        try:
            content = response['choices'][0]['message']['content']
            
            # Initialize result
            result = {
                'email': None,
                'confidence': 'low',
                'source': None,
                'thoughts': None,
                'raw_response': content
            }
            
            # Extract thoughts - try both <think> tags and reasoning sections
            thoughts = []
            
            # Try <think> tags
            think_matches = re.findall(r'<think>(.*?)</think>', content, re.DOTALL)
            if think_matches:
                thoughts.extend(think_matches)
            
            # Try numbered reasoning or steps
            step_matches = re.findall(r'\d+\.\s*(.*?)(?=\d+\.|$)', content, re.DOTALL)
            if step_matches:
                thoughts.extend(step_matches)
            
            if thoughts:
                result['thoughts'] = '\n'.join(t.strip() for t in thoughts)
            
            # Extract email
            if '@' in content:
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                if email_match:
                    result['email'] = email_match.group(0)
            
            # Extract confidence
            if 'confidence:' in content.lower():
                confidence_match = re.search(r'confidence:\s*(high|medium|low)', content, re.I)
                if confidence_match:
                    result['confidence'] = confidence_match.group(1).lower()
            elif 'high confidence' in content.lower():
                result['confidence'] = 'high'
            elif 'medium confidence' in content.lower():
                result['confidence'] = 'medium'
            
            # Extract source
            if 'source:' in content.lower():
                source_match = re.search(r'source:\s*(.+?)(?:\n|$)', content, re.I)
                if source_match:
                    result['source'] = source_match.group(1).strip()
            
            return result
            
        except (KeyError, IndexError) as e:
            print(f"Error parsing response: {e}")
            return {
                'email': None,
                'confidence': 'low',
                'source': None,
                'thoughts': None,
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
            "messages": [
                {
                    "role": "user",
                    "content": question
                }
            ],
            "stream": False,  # Set to False for single response
            "reasoning_effort": "high",  # Add reasoning effort parameter
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