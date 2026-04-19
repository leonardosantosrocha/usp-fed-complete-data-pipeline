import logging
import sys
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration


def setup_datasource(
    context, datasource_name, datasource_dir, asset_name, batching_regex
):
    datasource = context.sources.add_or_update_pandas_filesystem(
        name=datasource_name, base_directory=datasource_dir
    )
    existing_names = [a.name for a in datasource.assets]
    if asset_name not in existing_names:
        datasource.add_csv_asset(
            name=asset_name,
            batching_regex=batching_regex,
            header=0,
            sep=",",
        )
    return datasource


EXPECTED_COLUMNS = [
    "statefips", "state", "zipcode", "agi_stub", "year",
    "n1", "n2", "numdep", "mars1", "mars2", "mars4", "schf",
    "n00200", "n00300", "n00600", "n00650", "n00700", "n00900",
    "n01000", "n01400", "n01700", "n02300", "n02500", "n02650",
    "n02900", "n03150", "n03210", "n03220", "n03230", "n03240",
    "n03270", "n03300", "n04470", "n04800", "n05780", "n05800",
    "n06500", "n07100", "n07180", "n07220", "n07230", "n07240",
    "n07260", "n07300", "n09400", "n09600", "n09750", "n10300",
    "n10600", "n10960", "n11070", "n11560", "n11901", "n11902",
    "n18300", "n18425", "n18450", "n18500", "n19300", "n19700",
    "n26270", "n59660", "n59720", "n85300", "n85530", "n85770",
    "n85775",
    "a00100", "a00101", "a00200", "a00300", "a00600", "a00650",
    "a00700", "a00900", "a01000", "a01400", "a01700", "a02300",
    "a02500", "a02650", "a02900", "a03150", "a03210", "a03220",
    "a03230", "a03240", "a03270", "a03300", "a04470", "a04800",
    "a05780", "a05800", "a06500", "a07100", "a07180", "a07220",
    "a07230", "a07240", "a07260", "a07300", "a09400", "a09600",
    "a09750", "a10300", "a10600", "a10960", "a11070", "a11560",
    "a11901", "a11902", "a18300", "a18425", "a18450", "a18500",
    "a19300", "a19700", "a26270", "a59660", "a59720", "a85300",
    "a85530", "a85770", "a85775",
]


def setup_expectation_suite(context, name):
    suite = context.add_or_update_expectation_suite(name)

    # ----------------------------
    # 1. Schema: colunas esperadas
    # ----------------------------
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
            )
        )

    # ----------------------------
    # 2. Colunas-chave: não nulas
    # ----------------------------
    for col in ["statefips", "state", "zipcode", "agi_stub", "year"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
            )
        )

    # ----------------------------
    # 3. Valores categóricos
    # ----------------------------
    valid_fips = {i for i in range(1, 57)}
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "statefips", "value_set": sorted(valid_fips)},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "state",
                "value_set": [
                    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
                    "DC",
                ],
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "agi_stub", "value_set": [1, 2, 3, 4, 5, 6]},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "year", "value_set": [2014]},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={"column": "zipcode", "regex": r"^\d{5}$"},
        )
    )

    # ----------------------------
    # 4. Contagem >= 0
    # ----------------------------
    count_cols = [
        "n1", "n2", "numdep", "mars1", "mars2", "mars4", "schf",
        "n00200", "n00300", "n00600", "n00650", "n00700", "n00900",
        "n01000", "n01400", "n01700", "n02300", "n02500", "n02650",
        "n02900", "n03150", "n03210", "n03220", "n03230", "n03240",
        "n03270", "n03300", "n04470", "n04800", "n05780", "n05800",
        "n06500", "n07100", "n07180", "n07220", "n07230", "n07240",
        "n07260", "n07300", "n09400", "n09600", "n09750", "n10300",
        "n10600", "n10960", "n11070", "n11560", "n11901", "n11902",
        "n18300", "n18425", "n18450", "n18500", "n19300", "n19700",
        "n26270", "n59660", "n59720", "n85300", "n85530", "n85770",
        "n85775",
    ]
    for col in count_cols:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": col, "min_value": 0},
            )
        )

    # ----------------------------
    # 5. Valores monetários (A*): >= 0
    # ----------------------------
    can_be_negative = {"a00900", "a01000", "a26270"}

    all_amount_cols = [
        "a00100", "a00101", "a00200", "a00300", "a00600", "a00650",
        "a00700", "a00900", "a01000", "a01400", "a01700", "a02300",
        "a02500", "a02650", "a02900", "a03150", "a03210", "a03220",
        "a03230", "a03240", "a03270", "a03300", "a04470", "a04800",
        "a05780", "a05800", "a06500", "a07100", "a07180", "a07220",
        "a07230", "a07240", "a07260", "a07300", "a09400", "a09600",
        "a09750", "a10300", "a10600", "a10960", "a11070", "a11560",
        "a11901", "a11902", "a18300", "a18425", "a18450", "a18500",
        "a19300", "a19700", "a26270", "a59660", "a59720", "a85300",
        "a85530", "a85770", "a85775",
    ]
    for col in all_amount_cols:
        if col not in can_be_negative:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={"column": col, "min_value": 0},
                )
            )

    # ----------------------------
    # 6. Volume mínimo do dataset
    # ----------------------------
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 100_000, "max_value": 200_000},
        )
    )

    context.add_or_update_expectation_suite(expectation_suite=suite)
    return suite


def run_checkpoint(
    context, checkpoint_name, batch_request, expectation_suite_name
):
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    )
    run_name = datetime.now(tz=timezone.utc).strftime("irs_%Y%m%d__%H_%M_%S")
    return checkpoint.run(run_name=run_name, result_format="SUMMARY")


def log_results(context, results):
    for key, run_result in results.run_results.items():
        stats = run_result["validation_result"]["statistics"]
        evaluated = stats["evaluated_expectations"]
        unsuccessful = stats["unsuccessful_expectations"]

        for expectation_result in run_result["validation_result"]["results"]:
            expectation_type = expectation_result["expectation_config"][
                "expectation_type"
            ]
            column = expectation_result["expectation_config"]["kwargs"].get(
                "column", ""
            )
            status = "PASS" if expectation_result["success"] else "FAIL"
            logging.info(f"  [{status}] {expectation_type}({column})")

        for url in context.get_docs_sites_urls(resource_identifier=key):
            logging.info(
                f"Validation result: {url['site_url'].replace('%5C', '/')}"
            )

        if not results.success:
            logging.error(
                f"Validação falhou: {unsuccessful}/{evaluated} expectations não atendidas."
            )
            sys.exit(1)
        logging.info(
            f"Validação concluída com sucesso: {evaluated - unsuccessful}/{evaluated} expectations atendidas."
        )


def run(config: dict):
    logging.info("Starting raw data validation...")
    raw_path = config["raw"]["path"]
    expectation_suite_name = config["quality"]["expectation_suite_name"]
    asset_name = config["quality"]["asset_name"]
    datasource_name = config["quality"]["datasource_name"]
    checkpoint_name = config["quality"]["checkpoint_name"]

    context = gx.get_context(project_root_dir="./gx")

    datasource = setup_datasource(
        context, datasource_name, raw_path, asset_name, r".*\.csv"
    )
    batch_request = datasource.get_asset(asset_name).build_batch_request()

    setup_expectation_suite(context, expectation_suite_name)
    results = run_checkpoint(
        context, checkpoint_name, batch_request, expectation_suite_name
    )

    context.build_data_docs()
    log_results(context, results)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    run(
        {
            "raw": {"path": "/app/data/"},
            "quality": {
                "expectation_suite_name": "irs_income_tax_expectation_suite",
                "asset_name": "irs_income_tax_asset",
                "datasource_name": "irs_pandas_datasource",
                "checkpoint_name": "irs_income_tax_checkpoint",
            },
        }
    )
