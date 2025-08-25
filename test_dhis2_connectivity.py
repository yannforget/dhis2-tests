#!/usr/bin/env python3
"""
Test script to verify DHIS2 instance connectivity in CI.
"""

import time

import httpx


def test_dhis2_connectivity():
    """Test connectivity to DHIS2 instance."""
    base_url = "http://localhost:8080"
    max_retries = 30
    retry_delay = 10

    username = "admin"
    password = "district"

    print("Testing DHIS2 connectivity...")

    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=10) as client:
                response = client.get(
                    f"{base_url}/api/system/info", auth=(username, password)
                )

            if response.status_code == 200:
                info = response.json()
                print("✅ DHIS2 is running!")
                print(f"   Version: {info.get('version', 'Unknown')}")
                return True

        except httpx.RequestError as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Connection failed - {e}")

        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    print("❌ Failed to connect to DHIS2 instance")
    return False


if __name__ == "__main__":
    success = test_dhis2_connectivity()
