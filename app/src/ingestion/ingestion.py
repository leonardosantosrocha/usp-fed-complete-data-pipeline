
from dotenv import load_dotenv
import duckdb, os
load_dotenv()
from kaggle.api.kaggle_api_extended import KaggleApi

print(f"\n0. Bibliotecas e variáveis de ambiente importadas com sucesso...")


def authenticate_on_kaggle() -> object:

    api = KaggleApi()
    
    api.authenticate()

    print(f"\n1. Autenticação na plataforma Kaggle realizada com sucesso...")

    return api


def extract_data_from_kaggle(api : object, datasetName : str, toPath : str) -> None:

    try:

        api.dataset_download_files(
            dataset = datasetName,
            path = toPath,
            unzip = True,
        )

        print(f"\n2. Arquivos capturados do sistema origem e carregados no diretório './data/' com sucesso...")

    except Exception as e:

        print(f"\n2. Erro durante a captura dos arquivos do sistema origem: {e}")


def filter_required_file(fromPath : str, toPath : str) -> None:

    files = os.listdir(fromPath)

    for file in files:

        if file == "2014.csv":

            fromFilePath = fromPath + file

            toFilePath = toPath + f"tb-staging-individual-income-tax-{file[0:4]}.csv"

            os.rename(fromFilePath, toFilePath)

        else:
            
            deleted_file = os.path.join(fromPath, file)

            os.remove(deleted_file)

    print(f"\n3. Arquivo '{toFilePath.split('/')[-1]}' armazenado no diretório './data/' com sucesso...")


def authenticate_on_postgresql() -> object:

    connection = duckdb.connect()

    connection.execute("INSTALL postgres;")

    connection.execute("LOAD postgres;")

    connection_string = (
        f"host={os.getenv('POSTGRES_HOST')} "
        f"dbname={os.getenv('POSTGRES_DB')} "
        f"user={os.getenv('POSTGRES_USER')} "
        f"password={os.getenv('POSTGRES_PASSWORD')} "
        f"port={os.getenv('POSTGRES_PORT')}"
    )

    connection.execute(f"""ATTACH '{connection_string}' AS my_database (TYPE postgres);""")

    print(f"\n4. Autenticação no PostgreSQL realizada com sucesso...")

    return connection


def create_schema_on_postgresql(connection : object) -> None:

    query = """
    CREATE SCHEMA IF NOT EXISTS my_database.staging;
    """

    connection.execute(query)

    print(f"\n5. O schema 'staging' foi criado no PostgreSQL com sucesso...")


def create_table_on_postgresql(connection : object) -> None:

    query = """
    DROP TABLE IF EXISTS my_database.staging.tb_staging_individual_income_tax_2014 CASCADE;

    CREATE TABLE my_database.staging.tb_staging_individual_income_tax_2014
    (
        statefips TEXT,
        state TEXT,
        zipcode TEXT,
        agi_stub TEXT,
        n1 TEXT,
        mars1 TEXT,
        mars2 TEXT,
        mars4 TEXT,
        prep TEXT,
        n2 TEXT,
        numdep TEXT,
        total_vita TEXT,
        vita TEXT,
        tce TEXT,
        a00100 TEXT,
        n02650 TEXT,
        a02650 TEXT,
        n00200 TEXT,
        a00200 TEXT,
        n00300 TEXT,
        a00300 TEXT,
        n00600 TEXT,
        a00600 TEXT,
        n00650 TEXT,
        a00650 TEXT,
        n00700 TEXT,
        a00700 TEXT,
        n00900 TEXT,
        a00900 TEXT,
        n01000 TEXT,
        a01000 TEXT,
        n01400 TEXT,
        a01400 TEXT,
        n01700 TEXT,
        a01700 TEXT,
        schf TEXT,
        n02300 TEXT,
        a02300 TEXT,
        n02500 TEXT,
        a02500 TEXT,
        n26270 TEXT,
        a26270 TEXT,
        n02900 TEXT,
        a02900 TEXT,
        n03220 TEXT,
        a03220 TEXT,
        n03300 TEXT,
        a03300 TEXT,
        n03270 TEXT,
        a03270 TEXT,
        n03150 TEXT,
        a03150 TEXT,
        n03210 TEXT,
        a03210 TEXT,
        n03230 TEXT,
        a03230 TEXT,
        n03240 TEXT,
        a03240 TEXT,
        n04470 TEXT,
        a04470 TEXT,
        a00101 TEXT,
        n18425 TEXT,
        a18425 TEXT,
        n18450 TEXT,
        a18450 TEXT,
        n18500 TEXT,
        a18500 TEXT,
        n18300 TEXT,
        a18300 TEXT,
        n19300 TEXT,
        a19300 TEXT,
        n19700 TEXT,
        a19700 TEXT,
        n04800 TEXT,
        a04800 TEXT,
        n05800 TEXT,
        a05800 TEXT,
        n09600 TEXT,
        a09600 TEXT,
        n05780 TEXT,
        a05780 TEXT,
        n07100 TEXT,
        a07100 TEXT,
        n07300 TEXT,
        a07300 TEXT,
        n07180 TEXT,
        a07180 TEXT,
        n07230 TEXT,
        a07230 TEXT,
        n07240 TEXT,
        a07240 TEXT,
        n07220 TEXT,
        a07220 TEXT,
        n07260 TEXT,
        a07260 TEXT,
        n09400 TEXT,
        a09400 TEXT,
        n85770 TEXT,
        a85770 TEXT,
        n85775 TEXT,
        a85775 TEXT,
        n09750 TEXT,
        a09750 TEXT,
        n10600 TEXT,
        a10600 TEXT,
        n59660 TEXT,
        a59660 TEXT,
        n59720 TEXT,
        a59720 TEXT,
        n11070 TEXT,
        a11070 TEXT,
        n10960 TEXT,
        a10960 TEXT,
        n11560 TEXT,
        a11560 TEXT,
        n06500 TEXT,
        a06500 TEXT,
        n10300 TEXT,
        a10300 TEXT,
        n85530 TEXT,
        a85530 TEXT,
        n85300 TEXT,
        a85300 TEXT,
        n11901 TEXT,
        a11901 TEXT,
        n11902 TEXT,
        a11902 TEXT,
        year TEXT
    );
    """

    connection.execute(query)

    print(f"\n6. A tabela 'my_database.staging.tb_staging_individual_income_tax_2014' foi criada no PostgreSQL com sucesso...")


def insert_into_on_postgresql(connection : object) -> None:

    query = """
    INSERT INTO my_database.staging.tb_staging_individual_income_tax_2014
    (
        statefips,
        state,
        zipcode,
        agi_stub,
        n1,
        mars1,
        mars2,
        mars4,
        prep,
        n2,
        numdep,
        total_vita,
        vita,
        tce,
        a00100,
        n02650,
        a02650,
        n00200,
        a00200,
        n00300,
        a00300,
        n00600,
        a00600,
        n00650,
        a00650,
        n00700,
        a00700,
        n00900,
        a00900,
        n01000,
        a01000,
        n01400,
        a01400,
        n01700,
        a01700,
        schf,
        n02300,
        a02300,
        n02500,
        a02500,
        n26270,
        a26270,
        n02900,
        a02900,
        n03220,
        a03220,
        n03300,
        a03300,
        n03270,
        a03270,
        n03150,
        a03150,
        n03210,
        a03210,
        n03230,
        a03230,
        n03240,
        a03240,
        n04470,
        a04470,
        a00101,
        n18425,
        a18425,
        n18450,
        a18450,
        n18500,
        a18500,
        n18300,
        a18300,
        n19300,
        a19300,
        n19700,
        a19700,
        n04800,
        a04800,
        n05800,
        a05800,
        n09600,
        a09600,
        n05780,
        a05780,
        n07100,
        a07100,
        n07300,
        a07300,
        n07180,
        a07180,
        n07230,
        a07230,
        n07240,
        a07240,
        n07220,
        a07220,
        n07260,
        a07260,
        n09400,
        a09400,
        n85770,
        a85770,
        n85775,
        a85775,
        n09750,
        a09750,
        n10600,
        a10600,
        n59660,
        a59660,
        n59720,
        a59720,
        n11070,
        a11070,
        n10960,
        a10960,
        n11560,
        a11560,
        n06500,
        a06500,
        n10300,
        a10300,
        n85530,
        a85530,
        n85300,
        a85300,
        n11901,
        a11901,
        n11902,
        a11902,
        year
    )
    SELECT 
        statefips,
        state,
        zipcode,
        agi_stub,
        n1,
        mars1,
        mars2,
        mars4,
        prep,
        n2,
        numdep,
        total_vita,
        vita,
        tce,
        a00100,
        n02650,
        a02650,
        n00200,
        a00200,
        n00300,
        a00300,
        n00600,
        a00600,
        n00650,
        a00650,
        n00700,
        a00700,
        n00900,
        a00900,
        n01000,
        a01000,
        n01400,
        a01400,
        n01700,
        a01700,
        schf,
        n02300,
        a02300,
        n02500,
        a02500,
        n26270,
        a26270,
        n02900,
        a02900,
        n03220,
        a03220,
        n03300,
        a03300,
        n03270,
        a03270,
        n03150,
        a03150,
        n03210,
        a03210,
        n03230,
        a03230,
        n03240,
        a03240,
        n04470,
        a04470,
        a00101,
        n18425,
        a18425,
        n18450,
        a18450,
        n18500,
        a18500,
        n18300,
        a18300,
        n19300,
        a19300,
        n19700,
        a19700,
        n04800,
        a04800,
        n05800,
        a05800,
        n09600,
        a09600,
        n05780,
        a05780,
        n07100,
        a07100,
        n07300,
        a07300,
        n07180,
        a07180,
        n07230,
        a07230,
        n07240,
        a07240,
        n07220,
        a07220,
        n07260,
        a07260,
        n09400,
        a09400,
        n85770,
        a85770,
        n85775,
        a85775,
        n09750,
        a09750,
        n10600,
        a10600,
        n59660,
        a59660,
        n59720,
        a59720,
        n11070,
        a11070,
        n10960,
        a10960,
        n11560,
        a11560,
        n06500,
        a06500,
        n10300,
        a10300,
        n85530,
        a85530,
        n85300,
        a85300,
        n11901,
        a11901,
        n11902,
        a11902,
        year
    FROM 
        read_csv('../data/tb-staging-individual-income-tax-2014.csv')
    """

    connection.execute(query)

    print(f"\n7. Os dados foram inseridos na tabela 'my_database.staging.tb_staging_individual_income_tax_2014' com sucesso...")


def main():
    
    api = authenticate_on_kaggle()

    dataset_name = "irs/individual-income-tax-statistics"
    to_path = "../data/"
    extract_data_from_kaggle(api = api, datasetName = dataset_name, toPath = to_path)

    from_path = "../data/"
    to_path = "../data/"
    filter_required_file(fromPath = from_path, toPath = to_path)

    postgres_connection = authenticate_on_postgresql()
    create_schema_on_postgresql(postgres_connection)
    create_table_on_postgresql(postgres_connection)
    insert_into_on_postgresql(postgres_connection)
    postgres_connection.close()


if __name__ == "__main__":
    main()
