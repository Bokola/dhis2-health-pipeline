from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

# metadata parsing schema layout
METADATA_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("dataElements", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("code", StringType(), True),
        StructField("valueType", StringType(), True),
        StructField("domainType", StringType(), True),
        StructField("aggregationType", StringType(), True),
        StructField("zeroIsSignificant", BooleanType(), True),
        StructField("categoryCombo", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ]), True)
    ])), True),
    StructField("categoryOptionCombos", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])), True)
])

# organisation units schema map
ORG_UNITS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("organisationUnits", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("code", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("path", StringType(), True),
        StructField("parent", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("groups", ArrayType(StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])), True)
    ])), True)
])

# tracking program constraint sets
PROGRAMS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("programs", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("healthArea", StringType(), True),
        StructField("country", StringType(), True),
        StructField("reportingFrequency", StringType(), True),
        StructField("dataElements", ArrayType(StringType(), True))
    ])), True)
])

# data values container schema mapping
DATA_VALUES_SCHEMA = StructType([
    StructField("responseType", StringType(), True),
    StructField("version", StringType(), True),
    StructField("exportDate", StringType(), True),
    StructField("dataValues", ArrayType(StructType([
        StructField("dataElement", StringType(), True),
        StructField("period", StringType(), True),
        StructField("orgUnit", StringType(), True),
        StructField("categoryOptionCombo", StringType(), True),
        StructField("attributeOptionCombo", StringType(), True),
        StructField("value", StringType(), True),
        StructField("storedBy", StringType(), True),
        StructField("created", StringType(), True),
        StructField("lastUpdated", StringType(), True),
        StructField("followup", StringType(), True)
    ])), True)
])
