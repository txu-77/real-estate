from google.cloud import bigquery

RENTFASTER_SCORES_SCHEMA = {
    "scores": [
        bigquery.SchemaField("Category", "STRING"),
        bigquery.SchemaField("Score", "INTEGER"),
        bigquery.SchemaField("Count", "INTEGER"),
        bigquery.SchemaField(
            "Locations",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("AmenityID", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Address", "STRING"),
                bigquery.SchemaField("Category", "STRING"),
                bigquery.SchemaField("Dist", "INTEGER"),
                bigquery.SchemaField("Tag", "STRING"),
                bigquery.SchemaField("District", "STRING"),
                bigquery.SchemaField("WalkingTime", "INTEGER"),
                bigquery.SchemaField(
                    "Coord",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("Lat", "FLOAT"),
                        bigquery.SchemaField("Long", "FLOAT"),
                    ],
                ),
            ],
        ),
    ],
    "schools": [
        bigquery.SchemaField("Category", "STRING"),
        bigquery.SchemaField("Score", "INTEGER"),
        bigquery.SchemaField("Count", "INTEGER"),
        bigquery.SchemaField(
            "Locations",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("NCESDistrictID", "STRING"),
                bigquery.SchemaField("AmenityID", "STRING"),
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("Address", "STRING"),
                bigquery.SchemaField("Category", "STRING"),
                bigquery.SchemaField("Dist", "INTEGER"),
                bigquery.SchemaField("Tag", "STRING"),
                bigquery.SchemaField("District", "STRING"),
                bigquery.SchemaField("LowGrade", "STRING"),
                bigquery.SchemaField("HighGrade", "STRING"),
                bigquery.SchemaField("SchoolScore", "STRING"),
                bigquery.SchemaField("Site", "STRING"),
                bigquery.SchemaField("City", "STRING"),
                bigquery.SchemaField("StateAbbr", "STRING"),
                bigquery.SchemaField("ZIP", "STRING"),
                bigquery.SchemaField("Phone", "STRING"),
                bigquery.SchemaField("IsPre", "STRING"),
                bigquery.SchemaField("IsElem", "STRING"),
                bigquery.SchemaField("IsMid", "STRING"),
                bigquery.SchemaField("IsHigh", "STRING"),
                bigquery.SchemaField("ProfMath", "STRING"),
                bigquery.SchemaField("ProfLang", "STRING"),
                bigquery.SchemaField("WalkingTime", "INTEGER"),
                bigquery.SchemaField(
                    "Coord",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("Lat", "FLOAT"),
                        bigquery.SchemaField("Long", "FLOAT"),
                    ],
                ),
                bigquery.SchemaField("Assigned", "BOOLEAN"),
            ],
        ),
    ],
}

RENTFASTER_LOADJOBCONFIGS = {
    "rentals": {
        "schema": [
            bigquery.SchemaField("ref_id", "INTEGER"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("userId", "INTEGER"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("phone_2", "STRING"),
            bigquery.SchemaField("email", "BOOLEAN"),
            bigquery.SchemaField("availability", "STRING"),
            bigquery.SchemaField("a", "STRING"),
            bigquery.SchemaField("v", "INTEGER"),
            bigquery.SchemaField("f", "INTEGER"),
            bigquery.SchemaField("s", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("intro", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("community", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("marker", "STRING"),
            bigquery.SchemaField("link", "STRING"),
            bigquery.SchemaField("thumb2", "STRING"),
            bigquery.SchemaField("preferred_contact", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("price2", "STRING"),
            bigquery.SchemaField("beds", "STRING"),
            bigquery.SchemaField("beds2", "STRING"),
            bigquery.SchemaField("sq_feet", "STRING"),
            bigquery.SchemaField("sq_feet2", "STRING"),
            bigquery.SchemaField("baths", "STRING"),
            bigquery.SchemaField("baths2", "STRING"),
            bigquery.SchemaField("cats", "INTEGER"),
            bigquery.SchemaField("dogs", "INTEGER"),
            bigquery.SchemaField("utilities_included", "STRING", mode="REPEATED"),
            bigquery.SchemaField("dt", "DATE"),
        ],
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_disposition": "WRITE_TRUNCATE",
    },
    "rentals_scores": {
        "schema": [
            bigquery.SchemaField("ref_id", "INTEGER"),
            bigquery.SchemaField(
                "data",
                "RECORD",
                fields=[
                    bigquery.SchemaField(
                        "transit", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "shop", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "health", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "food", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "entertainment",
                        "RECORD",
                        fields=RENTFASTER_SCORES_SCHEMA["scores"],
                    ),
                    bigquery.SchemaField(
                        "coffee", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "grocery", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "fitness", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "childcare", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "park", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["scores"]
                    ),
                    bigquery.SchemaField(
                        "high", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["schools"]
                    ),
                    bigquery.SchemaField(
                        "elem", "RECORD", fields=RENTFASTER_SCORES_SCHEMA["schools"]
                    ),
                ],
            ),
            bigquery.SchemaField("is_active", "INTEGER"),
            bigquery.SchemaField("score", "INTEGER"),
            bigquery.SchemaField("dt", "DATE"),
        ],
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_disposition": "WRITE_TRUNCATE",
    },
}
