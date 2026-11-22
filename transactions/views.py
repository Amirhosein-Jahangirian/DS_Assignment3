from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .idempotency import (
    ACTION_DEPOSIT,
    ACTION_WITHDRAW,
    Replay,
    RequestConflict,
    check_replay,
    invalid_request_id_response,
    record,
    replay_after_integrity_error,
    valid_request_id,
)
from .models import Account
from .serializers import AccountDetailSerializer
from django.conf import settings
from django.db import connection, IntegrityError
from django.db import transaction
from django.http import JsonResponse
from .serializers import AmountInputSerializer, TransactionResponseSerializer
from .sharding import account_filter, owns


def _request_id_from(request):
    """
    Extract the client request_id, if any.

    A present-but-invalid request_id is rejected so a replay can never be
    mistaken for a new request. A missing request_id keeps the old behaviour
    (no idempotency guarantee can be offered without a client-provided key).
    """
    request_id = request.data.get('request_id')
    if request_id is not None and not valid_request_id(request_id):
        return None
    return request_id


def _not_owned_response(request, action, account_number):
    """
    Reject an account outside this shard's range.

    The same 404/UNKNOWN_ACCOUNT shape as a genuinely missing account is used
    on purpose: a shard must not reveal where another shard's accounts live.
    """
    request.transaction_data.update({
        'action': action,
        'account': account_number,
        'result': 'error:unknown_account',
        'balance': 'N/A',
    })
    return Response(
        {
            'status': 'error',
            'error_code': 'UNKNOWN_ACCOUNT',
            'message': f'Account {account_number} does not exist',
        },
        status=status.HTTP_404_NOT_FOUND,
    )


def health(request):
    """
    Liveness/readiness probe for the gateway and Docker.

    Checks the shard database with a trivial query so a replica whose database
    is unreachable is reported as unavailable. Deliberately outside /api/ so
    probes are not written to the transaction log.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1')
            cursor.fetchone()
    except Exception:
        return JsonResponse(
            {
                'status': 'error',
                'shard_id': settings.SHARD_ID,
                'replica_id': settings.REPLICA_ID,
                'db': 'unavailable',
            },
            status=503,
        )

    return JsonResponse(
        {
            'status': 'ok',
            'shard_id': settings.SHARD_ID,
            'replica_id': settings.REPLICA_ID,
            'db': 'ok',
        }
    )


class BalanceInquiryView(APIView):
    """
    Balance Inquiry API
    GET /api/accounts/<account_number>/

    Returns the current balance of the specified account.
    """

    def get(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'inquiry', account_number)

        try:
            account = Account.objects.get(account_number=account_number)
        except Account.DoesNotExist:
            # Populate transaction data for logging
            request.transaction_data.update({
                'action': 'inquiry',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })
            # Handles Section 2.3: Unknown account error
            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = AccountDetailSerializer(account)
        # Populate transaction data for logging
        request.transaction_data.update({
            'action': 'inquiry',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })
        return Response(serializer.data, status=status.HTTP_200_OK)


class AccountListView(APIView):
    """
    List Accounts API
    GET /api/accounts/

    Returns the accounts owned by this shard (its configured range only).
    A shard never lists accounts that belong to another shard.
    """

    def get(self, request):
        accounts = Account.objects.filter(account_filter()).order_by('account_number')
        serializer = AccountDetailSerializer(accounts, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class DepositView(APIView):
    """
    Deposit API
    POST /api/accounts/<account_number>/deposit/

    Adds the specified amount to the account balance.
    Guarantees ACID properties:
    - Atomicity: Uses transaction.atomic()
    - Consistency: Balance never negative (enforced by model validator)
    - Isolation: Uses select_for_update() to lock the row
    - Durability: PostgreSQL writes to disk before response
    """

    def post(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'deposit', account_number)

        # Step 1: Validate input (handles missing fields, invalid amounts)
        # time.sleep(1)  # TESTING ONLY: 100ms delay
        serializer = AmountInputSerializer(data=request.data)
        if not serializer.is_valid():
            request.transaction_data.update({
                'action': 'deposit',
                'account': account_number,
                'result': 'error:invalid_input',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'INVALID_INPUT',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        amount = serializer.validated_data['amount']
        request_id = _request_id_from(request)
        if request.data.get('request_id') is not None and request_id is None:
            return invalid_request_id_response()

        # Step 2: Execute transaction with ACID guarantees
        try:
            with transaction.atomic():
                # Lock the account row for Isolation (prevents lost updates)
                account = Account.objects.select_for_update().get(
                    account_number=account_number
                )

                # Experiment 7: a repeated request_id replays the recorded
                # response instead of applying the deposit again.
                if request_id is not None:
                    try:
                        check_replay(
                            request_id, ACTION_DEPOSIT,
                            f'{account_number}:{amount}',
                        )
                    except (Replay, RequestConflict) as replay_exc:
                        request.transaction_data.update({
                            'action': 'deposit',
                            'account': account_number,
                            'result': 'error:request_id_conflict'
                            if isinstance(replay_exc, RequestConflict)
                            else 'success:replayed',
                            'balance': 'N/A',
                        })
                        return replay_or_conflict_response(replay_exc, request_id)

                # Perform the deposit
                account.balance += amount
                account.save()  # Durability: PostgreSQL commits to disk

                # Build the response and record the ledger row in the SAME
                # transaction, so the effect and its idempotency marker are
                # committed together (never one without the other).
                response_serializer = TransactionResponseSerializer({
                    'status': 'success',
                    'account': account.account_number,
                    'balance': account.balance
                })
                response_body = response_serializer.data
                if request_id is not None:
                    record(request_id, ACTION_DEPOSIT,
                           f'{account_number}:{amount}', response_body)

        except Account.DoesNotExist:
            # Handles Section 2.3: Unknown account
            request.transaction_data.update({
                'action': 'deposit',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )
        except IntegrityError:
            replay_exc = replay_after_integrity_error(
                request_id, ACTION_DEPOSIT, f'{account_number}:{amount}'
            )
            if replay_exc is not None:
                return replay_or_conflict_response(replay_exc, request_id)
            raise

        request.transaction_data.update({
            'action': 'deposit',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })

        return Response(response_body, status=status.HTTP_200_OK)


class WithdrawView(APIView):
    """
    Withdraw API
    POST /api/accounts/<account_number>/withdraw/

    Subtracts the specified amount from the account balance.
    Guarantees ACID properties:
    - Atomicity: Uses transaction.atomic()
    - Consistency: Balance never negative (checked before withdrawal)
    - Isolation: Uses select_for_update() to lock the row
    - Durability: PostgreSQL writes to disk before response
    """

    def post(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'withdraw', account_number)

        # time.sleep(1)  # TESTING ONLY: 100ms delay
        # Step 1: Validate input (handles missing fields, invalid amounts)
        serializer = AmountInputSerializer(data=request.data)
        if not serializer.is_valid():
            
        
            request.transaction_data.update({
                'action': 'withdraw',
                'account': account_number,
                'result': 'error:invalid_input',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'INVALID_INPUT',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        amount = serializer.validated_data['amount']
        request_id = _request_id_from(request)
        if request.data.get('request_id') is not None and request_id is None:
            return invalid_request_id_response()

        # Step 2: Execute transaction with ACID guarantees
        try:
            with transaction.atomic():
                # Lock the account row for Isolation (prevents lost updates)
                account = Account.objects.select_for_update().get(
                    account_number=account_number
                )

                # Experiment 7: a repeated request_id replays the recorded
                # response instead of applying the withdrawal again.
                if request_id is not None:
                    try:
                        check_replay(
                            request_id, ACTION_WITHDRAW,
                            f'{account_number}:{amount}',
                        )
                    except (Replay, RequestConflict) as replay_exc:
                        request.transaction_data.update({
                            'action': 'withdraw',
                            'account': account_number,
                            'result': 'error:request_id_conflict'
                            if isinstance(replay_exc, RequestConflict)
                            else 'success:replayed',
                            'balance': 'N/A',
                        })
                        return replay_or_conflict_response(replay_exc, request_id)

                # Check for sufficient funds (Consistency: no negative balances)
                if account.balance < amount:
                    request.transaction_data.update({
                        'action': 'withdraw',
                        'account': account_number,
                        'result': 'error:insufficient_funds',
                        'balance': account.balance
                    })

                    return Response(
                        {
                            'status': 'error',
                            'error_code': 'INSUFFICIENT_FUNDS',
                            'message': f'Account {account_number} has insufficient funds. '
                                      f'Current balance: {account.balance}, requested: {amount}'
                        },
                        status=status.HTTP_422_UNPROCESSABLE_ENTITY
                    )

                # Perform the withdrawal
                account.balance -= amount
                account.save()  # Durability: PostgreSQL commits to disk

                response_serializer = TransactionResponseSerializer({
                    'status': 'success',
                    'account': account.account_number,
                    'balance': account.balance
                })
                response_body = response_serializer.data
                if request_id is not None:
                    record(request_id, ACTION_WITHDRAW,
                           f'{account_number}:{amount}', response_body)

        except Account.DoesNotExist:
            # Handles Section 2.3: Unknown account
            request.transaction_data.update({
                'action': 'withdraw',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })

            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )
        except IntegrityError:
            replay_exc = replay_after_integrity_error(
                request_id, ACTION_WITHDRAW, f'{account_number}:{amount}'
            )
            if replay_exc is not None:
                return replay_or_conflict_response(replay_exc, request_id)
            raise

        request.transaction_data.update({
            'action': 'withdraw',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })

        return Response(response_body, status=status.HTTP_200_OK)
