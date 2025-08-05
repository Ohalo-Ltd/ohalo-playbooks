"""
Mock Identity Provider (IdP) service for simulating enterprise identity claims
"""

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

# Mock user profiles - simulates what an IdP would normally encode in a JWT or SAML assertion
USER_PROFILES = {
    "sarah.mitchell@enterprise.com": {
        "email": "sarah.mitchell@enterprise.com",
        "name": "Sarah Mitchell",
        "role": "Project Manager",
        "employee_id": "E99055",
        "groups": ["pm", "management"],
    },
    "charlie@corp.com": {
        "email": "charlie@corp.com",
        "name": "Charlie Davis",
        "role": "HR-Manager",
        "employee_id": "E67890",
        "groups": [
            "hr",
            "hr_manager",
            "can_see_all_reviews",
        ],
    },
}

@router.get("/idp/me")
def get_identity(email: str = Query(..., description="User email for identity lookup")):
    """
    Simulate IdP identity endpoint - returns user identity claims
    In a real system, this would verify JWT/SAML tokens and return authenticated user info
    """
    user_profile = USER_PROFILES.get(email)
    if not user_profile:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user_profile

@router.get("/idp/users")
def list_users():
    """
    List all available mock users for testing purposes
    """
    return {
        "users": list(USER_PROFILES.keys()),
        "note": "Use any of these emails with /idp/me?email=<email> to simulate authentication"
    }
