## Requirements

Make sure the targeted public members list exists in the database

```SQL
INSERT INTO public.publishers_list (
    name,
    screen_name,
    locked,
    locked_at,
    unlocked_at,
    list_id,
    public_id,
    total_members,
    total_statuses,
    deleted_at,
    created_at
)
VALUES (
    'MEMBERS_LIST_NAME',
    '',
    false,
    null,
    now(),
    '1',
    gen_random_uuid(),
    0,
    0,
    null,
    now()
)
```