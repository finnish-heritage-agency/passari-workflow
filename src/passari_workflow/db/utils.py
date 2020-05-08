def bulk_create_or_get(session, mapper, ids):
    """
    For given model, return a list of entries with given primary keys.

    Any entries that don't exist will be created.
    """
    entries = list(
        session.query(mapper)
        .filter(mapper.id.in_(ids))
    )
    existing_ids = set([entry.id for entry in entries])
    missing_ids = set(ids) - existing_ids

    if missing_ids:
        session.bulk_insert_mappings(
            mapper,
            [{"id": missing_id} for missing_id in missing_ids]
        )
        entries += list(
            session.query(mapper)
            .filter(mapper.id.in_(missing_ids))
        )

    return entries
