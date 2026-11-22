from django.core.validators import MinValueValidator
from django.db import models


class Account(models.Model):

    account_number = models.CharField(
        max_length=20, 
        unique=True, 
        db_index=True,
    )
    balance = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        default=0.00,
        validators=[MinValueValidator(0.00)], # Enforces Consistency: No negative balances
    )
    is_active = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=models.Q(balance__gte=0),
                name='account_balance_non_negative',
            ),
        ]

    def __str__(self):
        return f"{self.account_number} | Balance: {self.balance}"


class ParticipantCommit(models.Model):
    """
    Durable commit marker for one 2PC participant GID.

    A row means this participant has durably accepted COMMIT for the GID.
    PostgreSQL's pg_prepared_xacts remains the authoritative PREPARED state;
    this table only makes repeated COMMIT idempotent and prevents a later
    ROLLBACK from undoing a recorded commit. It is not the coordinator's
    transaction state machine.
    """

    gid = models.CharField(max_length=200, primary_key=True)
    committed_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.gid


class ProcessedRequest(models.Model):
    """
    Durable idempotency ledger for shard-local operations (Experiment 7).

    One row per client request_id whose effect was applied on this shard.
    The row is written in the same database transaction as the balance
    change, so a request is either applied-and-recorded or neither. A repeat
    of the same request_id returns the stored response without applying the
    effect again. Repeats with a different action, accounts or amount are
    rejected as a conflict, mirroring the coordinator's replay check.
    """

    request_id = models.CharField(max_length=128, unique=True, db_index=True)
    action = models.CharField(max_length=16)
    detail = models.CharField(max_length=120, blank=True)
    response = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.request_id} | {self.action}"
