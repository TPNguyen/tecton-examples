from tecton import Entity

ad_entity = Entity(name="Ad", default_join_keys=["ad_id"], description="The ad")
user_entity = Entity(
    name="User",
    default_join_keys=["user_uuid"],
    description="A user on a given website. Users are fingerprinted based on device id, etc.",
)
