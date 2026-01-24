import asyncio
import logging

from kickpython import KickAPI
import os

logger = logging.getLogger(__name__)

async def authorize():
    api = KickAPI(
        client_id=os.getenv("KICK_CLIENT_ID", ""),
        client_secret=os.getenv("KICK_CLIENT_SECRET", ""),
        redirect_uri=os.getenv("KICK_REDIRECT_URI", "http://localhost:8080/callback"),
        db_path="kick_tokens.db"
    )

    """
    !!! IMPORTANT - READ ALL OF THESE COMMENTS !!!
    When you click the OAuth link given to you by main.py, in the URL you will see "?code=" somewhere.
    Copy that code and set the below variable to its value.
    """
    code = "PUT_THE_CODE_HERE"

    """
    When you first run main.py with Kick enabled, you will get auth instructions that tells you to OAuth your app with Kick.
    Set code_verifier to the value it provided you.
    """
    code_verifier = "PUT_THE_CODE_VERIFIER_HERE"

    """
    Once you've successfully done this and it works, hopefully you won't have to do it again.
    However, you may end up having to. Ideally *at worst* this will only be necessary every few weeks, if at all again.
    
    **This will not impact the stream in any capacity beyond causing failures to update the title and category.**
    """

    try:
        token_data = await api.exchange_code(code, code_verifier)
        logger.info("SUCCESS! Tokens exchanged:", token_data)
        await api.start_token_refresh()
        logger.info("Token refresh started. You can now run your main script.")
    except Exception as e:
        print("Error during exchange:", str(e))

if __name__ == "__main__":
    asyncio.run(authorize())