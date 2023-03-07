from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
password = "secure?password"

driver = GraphDatabase.driver(uri, auth=(user, password))


# Company Headers
company_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
FIELDTERMINATOR ';'
CREATE (:Company {CDCIA_USUARIA: row.CDCIA_USUARIA, DSCIA_USUARIA: row.DSCIA_USUARIA,NIT_COMPANIA: row.NIT_COMPANIA, DSTIPO_ACTIVIDAD:row.DSTIPO_ACTIVIDAD})
"""

# Country Headers
country_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row 
FIELDTERMINATOR ';'
CREATE (:Country {CountryCod: row.CountryCod, CountryName: row.CountryName, CountryID: row.CountryID})
"""

# Date Headers
date_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
FIELDTERMINATOR ';'
CREATE (:Date {DateYear: row.DateYear, DateMonth: row.DateMonth,DateDay: row.DateDay, DateHour:row.DateHour, DateWeekDay: row.DateWeekDay,DateWeekDayName: row.DateWeekDayName, DateMonthName: row.DateMonthName, DateId: row.DateId, Semester: row.Semester }) 
"""

# Importer Headers
importer_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row 
FIELDTERMINATOR ';'
CREATE (:Importer {NIT_IMPORTADOR: row.NIT_IMPORTADOR, NOMBRE_IMPORTADOR: row.NOMBRE_IMPORTADOR, ImporterID: row.ImporterID})
"""

# product_headers
product_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
FIELDTERMINATOR ';'
CREATE (:Product{COD_ITEM: row.COD_ITEM, ITEM: row.ITEM,TIPO_ITEM: row.TIPO_ITEM, UNIDAD_MEDIDA:row.UNIDAD_MEDIDA, ProductID: row.ProductID})
"""

# sia header
sia_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
FIELDTERMINATOR ';'
CREATE (:Sia {NIT_SIA: row.NIT_SIA, NOMBRE_SIA: row.NOMBRE_SIA, siaID: row.siaID})
"""

estado_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
FIELDTERMINATOR ';'
CREATE (:Status {CDESTADO: row.CDESTADO, DSESTADO: row.DSESTADO})
    """
# registry fact
registry_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
CREATE (:Facts {IdFact: row.IdFact, FechaRechazoID: row.FechaRechazoID,FechaAprobacionID: row.FechaAprobacionID, FechaDefinitivoID:row.FechaDefinitivoID, FechaRevisionID: row.FechaRevisionID,FechaDigitalizacionID: row.FechaDigitalizacionID, ProductoID: row.ProductoID, PaisOrigenID: row.PaisOrigenID, PaisBanderaID: row.PaisBanderaID, PaisDestinoID: row.PaisDestinoID, PaisProcedenciaID: row.PaisProcedenciaID, PaisCompraID: row.PaisCompraID, EmpresaID: row.EmpresaID, ImporterId: row.ImporterId, SiaID: row.SiaID, EstadoID: row.EstadoID, TipoTransaccionID: row.TipoTransaccionID, CANTIDAD: row.CANTIDAD, PRECIO: row.PRECIO, PESO_BRUTO: row.PESO_BRUTO, PESO_NETO: row.PESO_NETO, FLETES: row.FLETES, FOB: row.FOB })
"""
transactionType_query = """
LOAD CSV WITH HEADERS FROM $file_path AS row
CREATE (:TransactionType {CDTRANSACCION: row.CDTRANSACCION, DSTRANSACCION: row.DSTRANSACCION})
"""

queries = [
    company_query,
    country_query,
    date_query,
    importer_query,
    product_query,
    sia_query,
    estado_query,
    registry_query,
    transactionType_query,
]
company_path = "file:///DimCompany.csv"
country_path = "file:///DimCountry.csv"
date_path = "file:///DimDate.csv"
importer_path = "file:///DimImporter.csv"
product_path = "file:///DimProduct.csv"
sia_path = "file:///DimSia.csv"
status_path = "file:///DimStatus.csv"
fact_path = "file:///FactImportRegistry.csv"
transactionType_path = "file:///DimTransactionType.csv"


def load_csv_file(tx, file_path, query):
    tx.run(query, file_path=file_path)


def load_company(tx):
    tx.run(company_query, file_path=company_path)


def load_country(tx):
    tx.run(country_query, file_path=country_path)


def load_date(tx):
    tx.run(date_query, file_path=date_path)


def load_importer(tx):
    tx.run(importer_query, file_path=importer_path)


def load_product(tx):
    tx.run(product_query, file_path=product_path)


def load_sia(tx):
    tx.run(sia_query, file_path=sia_path)


def load_status(tx):
    tx.run(estado_query, file_path=status_path)


def load_facts(tx):
    tx.run(registry_query, file_path=fact_path)


def load_transactionType(tx):
    tx.run(transactionType_query, file_path=transactionType_path)


def drop_all(tx):
    query = """
   CALL apoc.periodic.iterate(
    "MATCH (n) RETURN n",
    "DETACH DELETE n",
    {batchSize: 2000, parallel: 'false'}
)
    """
    tx.run(query)


def create_facts_status_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (status:Status)
        WHERE fact.EstadoID = status.CDESTADO
        RETURN fact, status",

        "MERGE (fact)-[:status_transaction]->(status)",

        {batchSize:2000, parallel:false}
    )
    """
    tx.run(query)

def create_product_relationship(tx):
    query="""
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (product:Product)
        WHERE fact.ProductoID = product.ProductID
        RETURN fact, product",

        "MERGE (fact)-[:Tiene]->(product)",
        
        {batchSize:2000, parallel:false}
    )
    """
    tx.run(query)
    
def create_empresa_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (empresa:Company)
        WHERE fact.EmpresaID = empresa.CDCIA_USUARIA
        RETURN fact, empresa",
        "MERGE (fact)-[:Belongs]->(empresa)",
        {batchSize:2000, parallel:false}
    )
    """
    tx.run(query)

with driver.session() as session:
    # session.execute_write(drop_all)
    # session.execute_write(load_company)
    # session.execute_write(load_country)
    # session.execute_write(load_date)
    # session.execute_write(load_facts)
    # session.execute_write(load_importer)
    # session.execute_write(load_product)
    # session.execute_write(load_status)
    # session.execute_write(load_sia)
    # session.execute_write(load_transactionType)

    # session.execute_write(create_facts_status_relationship)
    # session.execute_write(create_product_relationship)
    session.execute_write(create_empresa_relationship)

