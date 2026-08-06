from TokenManager import TokenManager
import requests
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

client_id = str(os.getenv("OPENSKY_CLIENT_ID"))
client_secret = str(os.getenv("OPENSKY_CLIENT_SECRET"))


# Create a single shared instance for your script.
def main():
    tokens = TokenManager(client_id, client_secret)

    # Use it for any API call - the token is refreshed automatically.
    response = requests.get(
        "https://opensky-network.org/api/states/all",
        headers=tokens.headers(),
    )
    print(response.json())


if __name__ == "__main__":
    main()
