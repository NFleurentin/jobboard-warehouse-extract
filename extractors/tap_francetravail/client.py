"""REST client handling, including FranceTravailStream base class."""

from __future__ import annotations

import decimal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_francetravail import schemas
from tap_francetravail.auth import FranceTravailAuthenticator
from tap_francetravail.paginator import FranceTravailPaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Auth, Context
    from singer_sdk.streams.rest import HTTPRequest, PageContext


SCHEMAS_DIR = SchemaDirectory(schemas)

def normalize_record(record: dict, schema: dict) -> dict:
    schema_fields = set(schema.get("properties", {}).keys())
    extra = {}

    normalized = {}

    for key, value in record.items():
        if key in schema_fields:
            # Field defined in the schema
            field_schema = schema["properties"][key]

            # If it’s an object
            if isinstance(value, dict) and field_schema.get("type") == "object":
                normalized[key] = normalize_record(value, field_schema)

            # If it’s an array of objects
            elif isinstance(value, list) and field_schema.get("type") == "array":
                item_schema = field_schema.get("items", {})
                normalized[key] = [
                    normalize_record(item, item_schema)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]

            else:
                normalized[key] = value

        else:
            # Field not defined in the schema → in "_extra"
            extra[key] = value

    normalized["_extra"] = extra
    return normalized


class FranceTravailStream(RESTStream):
    """FranceTravail stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.resultats[*]"

    schema = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        """The API URL root, configurable via tap settings."""
        return "https://api.francetravail.io/partenaire/offresdemploi/v2"

    @override
    @cached_property
    def authenticator(self) -> Auth:
        """An authenticator object."""
        return FranceTravailAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint="https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire",
            oauth_scopes=self.config.get("scope", "api_offresdemploiv2 o2dsoffre"),
        )

    @property
    @override
    def http_headers(self) -> dict:
        """A dictionary of HTTP headers."""
        return {}

    @override
    def get_new_paginator(self) -> FranceTravailPaginator:
        return FranceTravailPaginator(start_value=0, page_size=150)

    @override
    def get_http_request(self, *, page: PageContext[Any]) -> HTTPRequest:
        """Return a request object for this stream.

        Args:
            page: An object containing the stream partition or context dictionary,
                and the next page token if applicable.

        Returns:
            An HTTP request for this stream.
        """
        request = super().get_http_request(page=page)

        return request

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        if response.status_code == 204 or not response.text.strip():
            return []

        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        return normalize_record(row, self.schema)
