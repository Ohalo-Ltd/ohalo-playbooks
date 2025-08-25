"""
Mock Identity Provider (IdP) service for simulating enterprise identity claims
"""

from fastapi import APIRouter, HTTPException, Query
from core.config import yaml_config

router = APIRouter()

def _get_user_profiles():
    """Get user profiles from the app configuration"""
    users = yaml_config.get_users()
    
    # Convert UserConfig objects to dictionaries for API compatibility
    profiles = {}
    for email, user_config in users.items():
        profiles[email] = {
            "email": user_config.email,
            "name": user_config.name,
            "role": user_config.role,
            "employee_id": user_config.employee_id,
            "groups": user_config.groups,
        }
    
    return profiles

@router.get("/idp/me")
def get_identity(email: str = Query(..., description="User email for identity lookup")):
    """
    Simulate IdP identity endpoint - returns user identity claims
    In a real system, this would verify JWT/SAML tokens and return authenticated user info
    """
    user_profiles = _get_user_profiles()
    user_profile = user_profiles.get(email)
    if not user_profile:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user_profile

@router.get("/idp/users")
def list_users():
    """
    List all available mock users for testing purposes
    """
    user_profiles = _get_user_profiles()
    return {
        "users": list(user_profiles.keys()),
        "note": "Use any of these emails with /idp/me?email=<email> to simulate authentication"
    }
