from tecton import Entity

user = Entity(
    name="fraud_user", 
    default_join_keys=["user_id"], 
    description="A user of the payment platform"
)
