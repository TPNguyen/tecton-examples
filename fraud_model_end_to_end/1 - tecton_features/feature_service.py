from tecton import FeatureService, FeaturesConfig
from features.transaction_aggregates import transaction_aggregates
from features.credit_score import users_credit_score
from features.since_last import days_since_last_transaction
from features.user_age import user_age_days

fraud_prediction_service = FeatureService(
    name='fraud_prediction_service',
    description='A FeatureService used for fraud detection.',
    online_serving_enabled=True,
    features=[
        transaction_aggregates,
        users_credit_score,
        days_since_last_transaction,
        user_age_days
    ]
)
