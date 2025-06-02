import os
import time
import json
import logging
import requests
import threading

class TokenManager:
    def __init__(self, output_dir: str, refresh_interval_minutes: int = 20):
        self.output_dir = output_dir
        self.token = None
        self.token_type = None
        self.lock = threading.Lock()
        self.refresh_interval = refresh_interval_minutes * 60  # secondes
        self.last_refresh = 0
        self._initialize_token()

    def _initialize_token(self):
        token, token_type = self._get_token()
        if token:
            self.token = token
            self.token_type = token_type
            self.last_refresh = time.time()
            logging.info("Token initialisé avec succès.")
        else:
            logging.error("Échec de l'initialisation du token.")

    def _get_credentials(self) -> dict:
        try:
            with open(os.path.join(self.output_dir, "clientCredentials.json"), "r") as f:
                return json.load(f)
        except Exception as e:
            logging.exception("Erreur lors de la lecture du fichier credentials")
            raise e

    def _get_token(self) -> tuple[str, str]:
        creds = self._get_credentials()
        client_id = creds["clientID"]
        client_secret = creds["key"]

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        params = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "o2dsoffre api_offresdemploiv2"
        }

        try:
            response = requests.post(
                url="https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            token_data = response.json()
            return token_data["access_token"], token_data["token_type"]
        except requests.exceptions.HTTPError as errh:
            logging.exception("HTTP Error lors de la demande de token")
            logging.exception(errh)
        except Exception as e:
            logging.exception("Erreur inconnue lors de la demande de token")
            logging.exception(e)
        return None, None

    def get_token(self) -> tuple[str, str]:
        with self.lock:
            if time.time() - self.last_refresh >= self.refresh_interval:
                logging.info("Rafraîchissement du token...")
                self._initialize_token()
            return self.token, self.token_type