from fastapi import FastAPI, Header
import logging
import urllib.parse
import pprint
from models import *
import base64
import dataclasses

logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

@dataclasses.dataclass
class User:
    site_id: str


def get_user(api_key: str) -> User:
    # this should obviously come from a database !
    if api_key == "site-a-api-key":
        return User(site_id="site-a-id-followed-by-high-entropy-uuid")

    if api_key == "site-b-api-key":
        return User(site_id="site-b-id-followed-by-high-entropy-uuid")

    return None



@app.post("/user/get-profile")  # this is a POST and not a GET because we want to same kind of payload as for other routes
def get_user_profile(user_profile_request: UserProfileRequest):
    logging.info(f"get user profile from token '{user_profile_request.token_key}'")

    anonymous_profile = UserProfileResponse(
                name="Anonymous",
                permissions=[],
                authorized_labels=[],
                validity=1
            )

    if user_profile_request.token_key == "api-key":
        user = get_user(user_profile_request.token_value)
        # pprint.pprint(user)
        
        if not user:
            return anonymous_profile

        response = UserProfileResponse(
            name=user.site_id,
            permissions=[UserPermissions.UPLOAD, UserPermissions.VIEW, UserPermissions.DOWNLOAD],  # the gateway users can only upload and view/download studies that are labelled with their site_id
            authorized_labels=[user.site_id],
            validity=60)

        # pprint.pprint(response)

        return response

    return anonymous_profile
