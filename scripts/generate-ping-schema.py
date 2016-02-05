#!/usr/bin/env python3

import sys, json
from collections import OrderedDict
from urllib.request import urlopen

def print_help():
    print("""\
Generates Avro SchemaBuilder code in Scala automatically from a JSON Schema.

Usage:

    python3 generate-ping-schema.py URL_TO_JSON_SCHEMA

Example:

    python3 generate-ping-schema.py https://raw.githubusercontent.com/SamPenrose/data-pipeline/v4-baseset-schema/schemas/test/environment.schema.json > ping-schema.scala

The above results in the following Scala code being written to `ping-schema.scala`:

    SchemaBuilder
      .record("environment").fields()
        [...]
        .name("settings").`type`().record("settings").fields()
          .name("blocklistEnabled").`type`().optional().booleanType()
          .name("isDefaultBrowser").`type`().optional().booleanType()
          .name("defaultSearchEngine").`type`().optional().stringType()
          .name("defaultSearchEngineData").`type`().optional().record("defaultSearchEngineData").fields()
            .name("name").`type`().optional().stringType()
            .name("loadPath").`type`().optional().stringType()
            .name("submissionURL").`type`().optional().stringType()
          .endRecord()
          .name("e10sEnabled").`type`().optional().booleanType()
          .name("locale").`type`().optional().stringType()
          .name("telemetryEnabled").`type`().optional().booleanType()
          .name("update").`type`().optional().record("update").fields()
            .name("autoDownload").`type`().optional().booleanType()
            .name("channel").`type`().optional().stringType()
            .name("enabled").`type`().optional().booleanType()
          .endRecord()
          .name("userPrefs").`type`().optional().record("userPrefs").fields()

          .endRecord()
        .endRecord()
        [...]
      .endRecord()
""")

def convert_entry(schema, type_name, level, is_optional):
    if isinstance(schema, str): # primitive, named type
        return """.{actual_type}Type(){default}""".format(
            spacing = "  " * level, type_name = type_name,
            is_optional = ".optional()" if is_optional else "",
            default = "" if is_optional else ".noDefault()",
            actual_type = schema["type"]
        )

    # skip the things we don't know how to handle
    if "type" not in schema: return "<INVALID>"
    if schema["type"] == "array" or "array" in set(schema["type"]): return """<ARRAY>"""

    if set(schema["type"]) in [{"boolean", "null"}, {"int", "null"}, {"long", "null"}, {"float", "null"}, {"double", "null"}, {"bytes", "null"}, {"string", "null"}]:
        return """.optional().{actual_type}Type()""".format(
            spacing = "  " * level, type_name = type_name,
            actual_type = next(x for x in schema["type"] if x != "null")
        )
    if schema["type"] == "integer":
        return """{is_optional}.intType(){default}""".format(
            spacing = "  " * level, type_name = type_name,
            is_optional = ".optional()" if is_optional else "",
            default = "" if is_optional else ".noDefault()"
        )
    if schema["type"] == "number":
        return """{is_optional}.stringType(){default}""".format(
            spacing = "  " * level, type_name = type_name,
            is_optional = ".optional()" if is_optional else "",
            default = "" if is_optional else ".noDefault()"
        )
    if schema["type"] in {"boolean", "int", "long", "float", "double", "bytes", "string"}:
        return """{is_optional}.{actual_type}Type(){default}""".format(
            spacing = "  " * level, type_name = type_name,
            is_optional = ".optional()" if is_optional else "",
            default = "" if is_optional else ".noDefault()",
            actual_type = schema["type"]
        )

    if schema["type"] == "object":
        return """\
{is_optional}.record("{type_name}").fields()
{field_types}
{spacing}.endRecord()""".format(
            spacing = "  " * level, type_name = type_name,
            is_optional = ".optional()" if is_optional else "",
            default = "" if is_optional else ".noDefault()",
            field_types = "\n".join(
                """{spacing}.name("{type_name}").`type`(){field_definition}""".format(
                    spacing = "  " * (level + 1), type_name = name,
                    field_definition = convert_entry(
                        subschema, name, level + 1,
                        name not in schema.get("required", [])
                    )
                )
                for name, subschema in schema.get("properties", {}).items()
            )
        )

    raise ValueError("Unknown schema type: {}".format(schema))

def convert(schema, schema_name):
    return """SchemaBuilder
  {result}""".format(
        result = convert_entry(schema, schema_name, 1, False)
    )

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print_help()
        sys.exit()
    schema_url = sys.argv[1]

    response_text = urlopen(schema_url).read().decode("utf-8")
    response = json.loads(response_text, object_pairs_hook=OrderedDict)
    for schema_name, schema in response.items():
        print(convert(schema, schema_name))
