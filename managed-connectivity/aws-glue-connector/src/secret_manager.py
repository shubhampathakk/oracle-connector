from google.cloud import secretmanager
import json

class SecretManager:
    """A helper class for accessing secrets from Google Secret Manager."""

    @staticmethod
    def get_aws_credentials(project_id, secret_id):
        """
        Retrieves AWS credentials from Secret Manager.
        The secret should be a JSON string with 'access_key_id' and 'secret_access_key'.
        """
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=name)
        payload = response.payload.data.decode("UTF-8")
        credentials = json.loads(payload)
        return credentials['access_key_id'], credentials['secret_access_key']