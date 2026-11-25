import json
import os
import stat
import sys
import types

from lakefreight import store
from lakefreight.config import default_config_text, load_settings


def test_credentials_from_environment(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_SECRET_ARN", raising=False)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acme-x1")
    monkeypatch.setenv("SNOWFLAKE_USER", "LAKEFREIGHT")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "/keys/k.p8")
    assert store.credentials() == {
        "account": "acme-x1",
        "user": "LAKEFREIGHT",
        "private_key_file": "/keys/k.p8",
    }


def test_credentials_from_secrets_manager(monkeypatch):
    secret = {"account": "acme-x1", "user": "LAKEFREIGHT", "private_key": "-----BEGIN KEY-----"}
    calls = []

    class FakeClient:
        def get_secret_value(self, SecretId):
            calls.append(SecretId)
            return {"SecretString": json.dumps(secret)}

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda _: FakeClient()))
    monkeypatch.setenv("SNOWFLAKE_SECRET_ARN", "arn:aws:secretsmanager:us-east-2:1:secret:sf")
    monkeypatch.setattr(store, "_KEY_FILE", None)
    creds = store.credentials()
    assert calls == ["arn:aws:secretsmanager:us-east-2:1:secret:sf"]
    assert creds["account"] == "acme-x1" and creds["user"] == "LAKEFREIGHT"
    with open(creds["private_key_file"]) as f:
        assert f.read() == "-----BEGIN KEY-----"
    assert stat.S_IMODE(os.stat(creds["private_key_file"]).st_mode) == 0o600
    assert store.credentials()["private_key_file"] == creds["private_key_file"]  # reused
    os.remove(creds["private_key_file"])


def test_settings_from_environment_toml(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LAKEFREIGHT_CONFIG", raising=False)
    text = default_config_text()
    text = text.replace('prefix = "internal"', 'prefix = "internal"\nkms_key_id = "alias/k"')
    monkeypatch.setenv("LAKEFREIGHT_CONFIG_TOML", text)
    settings = load_settings()
    assert settings.zones["INTERNAL"].kms_key_id == "alias/k"
    assert settings.zones["PII"].kms_key_id is None
