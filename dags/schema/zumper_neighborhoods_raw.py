from google.cloud import bigquery

ZUMPER_NEIGHBORHOODS_LOADJOBCONFIG = {
    "schema": [
        bigquery.SchemaField("neighborhood_id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("city_state_name", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField(
            "media",
            "STRING",
            mode="REPEATED",
        ),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lng", "FLOAT"),
        bigquery.SchemaField("color", "STRING"),
        bigquery.SchemaField(
            "box",
            "FLOAT",
            mode="REPEATED",
        ),
        bigquery.SchemaField("city_id", "INTEGER"),
        bigquery.SchemaField("html", "STRING"),
        bigquery.SchemaField(
            "data",
            "RECORD",
            fields=[
                bigquery.SchemaField(
                    "nearby",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "cities",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("path", "STRING"),
                            ],
                        ),
                        bigquery.SchemaField(
                            "universities",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("path", "STRING"),
                            ],
                        ),
                        bigquery.SchemaField(
                            "neighborhoods",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("path", "STRING"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "verticals",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "apartments",
                            "STRING",
                            mode="REPEATED",
                        ),
                        bigquery.SchemaField(
                            "rooms",
                            "STRING",
                            mode="REPEATED",
                        ),
                        bigquery.SchemaField(
                            "houses",
                            "STRING",
                            mode="REPEATED",
                        ),
                        bigquery.SchemaField(
                            "condos",
                            "STRING",
                            mode="REPEATED",
                        ),
                        bigquery.SchemaField("rent_research", "BOOLEAN"),
                    ],
                ),
            ],
        ),
    ]
}
