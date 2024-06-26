from google.cloud import bigquery

HONESTDOOR_LOADJOBCONFIGS = {
    "properties": {
        "schema": [
            bigquery.SchemaField(
                "meta",
                "RECORD",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                ],
            ),
            bigquery.SchemaField("listingId", "STRING"),
            bigquery.SchemaField("slug", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("transactionType", "STRING"),
            bigquery.SchemaField("propertyType", "STRING"),
            bigquery.SchemaField("ownershipType", "STRING"),
            bigquery.SchemaField("updatedAt", "STRING"),
            bigquery.SchemaField("createdAt", "STRING"),
            bigquery.SchemaField("property", "STRING"),
            bigquery.SchemaField(
                "address",
                "RECORD",
                fields=[
                    bigquery.SchemaField("streetAddress", "STRING"),
                    bigquery.SchemaField("city", "STRING"),
                    bigquery.SchemaField("province", "STRING"),
                    bigquery.SchemaField("neighbourhood", "STRING"),
                    bigquery.SchemaField("cityId", "STRING"),
                    bigquery.SchemaField("neighbourhoodId", "STRING"),
                    bigquery.SchemaField("postalCode", "STRING"),
                ],
            ),
            bigquery.SchemaField(
                "building",
                "RECORD",
                fields=[
                    bigquery.SchemaField("bedroomsTotal", "INTEGER"),
                    bigquery.SchemaField("bathroomTotal", "INTEGER"),
                    bigquery.SchemaField("type", "STRING"),
                    bigquery.SchemaField("sizeInterior", "STRING"),
                    bigquery.SchemaField("halfBathTotal", "INTEGER"),
                    bigquery.SchemaField("constructedDate", "INTEGER"),
                    bigquery.SchemaField("sizeExterior", "STRING"),
                    bigquery.SchemaField("fireplacePresent", "STRING"),
                    bigquery.SchemaField("basementDevelopment", "STRING"),
                    bigquery.SchemaField("coolingType", "STRING"),
                    bigquery.SchemaField("storiesTotal", "INTEGER"),
                ],
            ),
            bigquery.SchemaField(
                "photo",
                "RECORD",
                fields=[
                    bigquery.SchemaField(
                        "propertyPhoto",
                        "RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField("largePhotoUrl", "STRING"),
                            bigquery.SchemaField("sequenceId", "STRING"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField(
                "agentDetails",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField(
                        "office",
                        "RECORD",
                        fields=[
                            bigquery.SchemaField("name", "STRING"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField(
                "location",
                "RECORD",
                fields=[
                    bigquery.SchemaField("lat", "FLOAT"),
                    bigquery.SchemaField("lon", "FLOAT"),
                ],
            ),
            bigquery.SchemaField(
                "land",
                "RECORD",
                fields=[
                    bigquery.SchemaField("sizeTotal", "STRING"),
                    bigquery.SchemaField("sizeTotalText", "STRING"),
                ],
            ),
            bigquery.SchemaField("dt", "DATE"),
        ],
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_disposition": "WRITE_TRUNCATE",
    },
    "property": {
        "schema": [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("slug", "STRING"),
            bigquery.SchemaField("assessmentClass", "STRING"),
            bigquery.SchemaField("zoning", "STRING"),
            bigquery.SchemaField("accountNumber", "STRING"),
            bigquery.SchemaField(
                "creaListing",
                "RECORD",
                fields=[
                    bigquery.SchemaField(
                        "meta",
                        "RECORD",
                        fields=[
                            bigquery.SchemaField("id", "STRING"),
                        ],
                    ),
                    bigquery.SchemaField("slug", "STRING"),
                    bigquery.SchemaField("price", "FLOAT"),
                    bigquery.SchemaField(
                        "location",
                        "RECORD",
                        fields=[
                            bigquery.SchemaField("lat", "FLOAT"),
                            bigquery.SchemaField("lon", "FLOAT"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField("bathroomsTotal", "FLOAT"),
            bigquery.SchemaField("bedroomsTotal", "INTEGER"),
            bigquery.SchemaField("livingArea", "INTEGER"),
            bigquery.SchemaField("lotSizeArea", "FLOAT"),
            bigquery.SchemaField("yearBuiltActual", "INTEGER"),
            bigquery.SchemaField("show", "BOOLEAN"),
            bigquery.SchemaField("fireplace", "STRING"),
            bigquery.SchemaField("garageSpaces", "INTEGER"),
            bigquery.SchemaField("houseStyle", "STRING"),
            bigquery.SchemaField("livingAreaUnits", "INTEGER"),
            bigquery.SchemaField("basement", "STRING"),
            bigquery.SchemaField("building", "STRING"),
            bigquery.SchemaField("bathroomsTotalEst", "FLOAT"),
            bigquery.SchemaField("bedroomsTotalEst", "INTEGER"),
            bigquery.SchemaField("livingAreaEst", "INTEGER"),
            bigquery.SchemaField("lotSizeAreaEst", "INTEGER"),
            bigquery.SchemaField("closeDate", "TIMESTAMP"),
            bigquery.SchemaField("closePrice", "INTEGER"),
            bigquery.SchemaField("lastEstimatedPrice", "INTEGER"),
            bigquery.SchemaField("lastEstimatedYear", "INTEGER"),
            bigquery.SchemaField("predictedDate", "TIMESTAMP"),
            bigquery.SchemaField("predictedValue", "INTEGER"),
            bigquery.SchemaField("unparsedAddress", "STRING"),
            bigquery.SchemaField("province", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("cityName", "STRING"),
            bigquery.SchemaField("neighbourhood", "STRING"),
            bigquery.SchemaField("neighbourhoodName", "STRING"),
            bigquery.SchemaField("postal", "STRING"),
            bigquery.SchemaField(
                "location",
                "RECORD",
                fields=[
                    bigquery.SchemaField("lat", "FLOAT"),
                    bigquery.SchemaField("lon", "FLOAT"),
                ],
            ),
            bigquery.SchemaField(
                "valuations",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("predictedValue", "INTEGER"),
                    bigquery.SchemaField("predictedDate", "TIMESTAMP"),
                ],
            ),
            bigquery.SchemaField(
                "assessments",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("value", "INTEGER"),
                    bigquery.SchemaField("year", "INTEGER"),
                ],
            ),
            bigquery.SchemaField(
                "rentalActual",
                "RECORD",
                fields=[
                    bigquery.SchemaField("rentEstimate", "INTEGER"),
                    bigquery.SchemaField("rentalYield", "FLOAT"),
                ],
            ),
            bigquery.SchemaField(
                "airbnbActual",
                "RECORD",
                fields=[
                    bigquery.SchemaField("airbnbEstimate", "INTEGER"),
                ],
            ),
            bigquery.SchemaField(
                "closes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("price", "FLOAT"),
                    bigquery.SchemaField("date", "TIMESTAMP"),
                ],
            ),
            bigquery.SchemaField("dt", "DATE"),
        ],
        "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "write_disposition": "WRITE_TRUNCATE",
    },
}
