from powerbi import PowerBI, PowerBIApi
import asyncio


async def main():
    deppy = PowerBI(
        api=PowerBIApi(
            base_url="https://api.powerbi.com/v1.0/myorg",
            access_token_url="https://login.microsoftonline.com/common/oauth2/token",
            client_id="client_id",
            client_secret="client_secret",
            scope="https://graph.microsoft.com/.default"
        ),
        refreshes_top=10
    )
    deppy.dot("powerbi_workflow.dot")

if __name__ == "__main__":
    asyncio.run(main())
