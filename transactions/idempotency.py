"""Shard-local idempotency ledger (Experiment 7: repeated client requests).

A repeated request carrying the same ``request_id`` must not produce a
duplicate effect. The ``ProcessedRequest`` table stores, in the same
database transaction as the balance change, the response body returned for
that request_id. A replay returns the stored response; a repeat with a
different action, accounts or amount is rejected as a conflict, mirroring
the coordinator's ``_check_replay`` semantics for cross-shard transfers.

Lock order across all three local paths is always account row(s) first and
ledger row second, so concurrent paths cannot deadlock each other.
"""
from __future__ import annotations

import re

from django.db import IntegrityError
from rest_framework import status
from rest_framework.response import Response

from .models import ProcessedRequest

REQUEST_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{8,128}$')

ACTION_DEPOSIT = 'deposit'
ACTION_WITHDRAW = 'withdraw'
ACTION_LOCAL_TRANSFER = 'local_transfer'


def valid_request_id(value) -> bool:
    """Match the gateway's request_id format (8-128 chars)."""
    return isinstance(value, str) and REQUEST_ID_PATTERN.fullmatch(value) is not None


def invalid_request_id_response() -> Response:
    return Response(
        {
            'status': 'error',
            'error_code': 'INVALID_REQUEST_ID',
            'message': (
                'request_id must be 8-128 characters using letters, digits, '
                '"_" or "-"'
            ),
        },
        status=status.HTTP_400_BAD_REQUEST,
    )


def conflict_response(detail: str) -> Response:
    return Response(
        {
            'status': 'error',
            'error_code': 'REQUEST_ID_CONFLICT',
            'message': (
                'This request_id was already used for a different operation '
                f'({detail})'
            ),
        },
        status=status.HTTP_409_CONFLICT,
    )


class Replay(Exception):
    """A recorded request_id was replayed; carry its stored response body."""

    def __init__(self, response_body: dict):
        self.response_body = response_body


class RequestConflict(Exception):
    """The request_id was recorded for a different operation."""


def check_replay(request_id: str, action: str, detail: str) -> None:
    """
    Raise Replay if this request_id was already applied with the same
    detail, RequestConflict if it was applied with a different one.
    Call inside transaction.atomic() after the account rows are locked.
    """
    recorded = (
        ProcessedRequest.objects
        .select_for_update()
        .filter(request_id=request_id)
        .first()
    )
    if recorded is None:
        return
    if recorded.action != action or recorded.detail != detail:
        raise RequestConflict()
    raise Replay(recorded.response)


def record(request_id: str, action: str, detail: str, response_body: dict) -> None:
    """Store the response for an applied request_id. Call inside the same
    transaction.atomic() block that applied the effect."""
    ProcessedRequest.objects.create(
        request_id=request_id,
        action=action,
        detail=detail,
        response=dict(response_body),
    )


def replay_or_conflict_response(exc: Exception, request_id: str) -> Response:
    """Build the response for a Replay or RequestConflict exception."""
    if isinstance(exc, Replay):
        return Response(exc.response_body, status=status.HTTP_200_OK)
    return conflict_response(request_id)


def replay_after_integrity_error(request_id: str, action: str, detail: str):
    """
    Concurrent duplicates may both pass the replay check and race on the
    ledger insert; the loser rolls back and re-reads the winner's row.
    Returns the stored row, or None if the row vanished (re-raise instead).
    """
    recorded = ProcessedRequest.objects.filter(request_id=request_id).first()
    if recorded is not None:
        if recorded.action != action or recorded.detail != detail:
            return RequestConflict()
        return Replay(recorded.response)
    return None
