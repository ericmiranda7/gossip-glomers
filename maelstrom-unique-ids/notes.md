Keeping in mind that IDs require to be only unique and not sortable,
there are a few design choices available
1. just use a UUIDv4 library (collisions yes, but minimal)
2. flickr ticket server (sql db auto increment)
3. mongodb objectid
4. twitter snowflake (and spinoffs thereof)

i'll try out the simplest first (uuid)
