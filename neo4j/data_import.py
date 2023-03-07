from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
password = "secure?password"

driver = GraphDatabase.driver(uri, auth=(user, password))


def load_company(tx):
    company_path = "file:///DimCompany.csv"
    company_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{company_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Company {{CDCIA_USUARIA: row.CDCIA_USUARIA, DSCIA_USUARIA: row.DSCIA_USUARIA,NIT_COMPANIA: row.NIT_COMPANIA, DSTIPO_ACTIVIDAD:row.DSTIPO_ACTIVIDAD}})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
        
    )
    """
    tx.run(company_query, file_path=company_path)


def load_country(tx):
    country_path = "file:///DimCountry.csv"
    country_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{country_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Country {{CountryCod: row.CountryCod, CountryName: row.CountryName, CountryID: row.CountryID}})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
    """
    tx.run(country_query, file_path=country_path)


def load_date(tx):
    date_path = "file:///DimDate.csv"
    date_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{date_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Date {{DateYear: row.DateYear, DateMonth: row.DateMonth,DateDay: row.DateDay, DateHour:row.DateHour, DateWeekDay: row.DateWeekDay,DateWeekDayName: row.DateWeekDayName, DateMonthName: row.DateMonthName, DateId: row.DateId, Semester: row.Semester }}) ',

        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
    """
    tx.run(date_query, file_path=date_path)


def load_importer(tx):
    importer_path = "file:///DimImporter.csv"
    importer_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{importer_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Importer {{NIT_IMPORTADOR: row.NIT_IMPORTADOR, NOMBRE_IMPORTADOR: row.NOMBRE_IMPORTADOR, ImporterID: row.ImporterID}})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
    """
    tx.run(importer_query, file_path=importer_path)


def load_product(tx):
    product_path = "file:///DimProduct.csv"
    product_query = f"""
    CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{product_path}" AS row FIELDTERMINATOR ";"
            RETURN row',

            'CREATE (:Product{{COD_ITEM: row.COD_ITEM, ITEM: row.ITEM,TIPO_ITEM: row.TIPO_ITEM, UNIDAD_MEDIDA:row.UNIDAD_MEDIDA, ProductID: row.ProductID}})',

            {{batchSize: 2000, parallel: false, iterateList: true}}
        )
    """
    tx.run(product_query, file_path=product_path)


def load_sia(tx):
    sia_path = "file:///DimSia.csv"
    sia_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{sia_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Sia {{NIT_SIA: row.NIT_SIA, NOMBRE_SIA: row.NOMBRE_SIA, siaID: row.siaID}})',
        
        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
    """

    # sia_query = """
    # LOAD CSV WITH HEADERS FROM $file_path AS row
    # FIELDTERMINATOR ';'
    # CREATE (:Sia {NIT_SIA: row.NIT_SIA, NOMBRE_SIA: row.NOMBRE_SIA, siaID: row.siaID})
    # """
    tx.run(sia_query, file_path=sia_path)


def load_status(tx):
    status_path = "file:///DimStatus.csv"
    estado_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{status_path}" AS row FIELDTERMINATOR ";"
        RETURN row',

        'CREATE (:Status {{CDESTADO: row.CDESTADO, DSESTADO: row.DSESTADO}})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
        
    )
        """
    tx.run(estado_query, file_path=status_path)


def load_facts(tx):
    fact_path = "file:///FactImportRegistry.csv"
    registry_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{fact_path}" AS row
        RETURN row',

        'CREATE (:Facts {{IdFact: row.IdFact, FechaRechazoID: row.FechaRechazoID,FechaAprobacionID: row.FechaAprobacionID, FechaDefinitivoID:row.FechaDefinitivoID, FechaRevisionID: row.FechaRevisionID,FechaDigitalizacionID: row.FechaDigitalizacionID, ProductoID: row.ProductoID, PaisOrigenID: row.PaisOrigenID, PaisBanderaID: row.PaisBanderaID, PaisDestinoID: row.PaisDestinoID, PaisProcedenciaID: row.PaisProcedenciaID, PaisCompraID: row.PaisCompraID, EmpresaID: row.EmpresaID, ImporterId: row.ImporterId, SiaID: row.SiaID, EstadoID: row.EstadoID, TipoTransaccionID: row.TipoTransaccionID, CANTIDAD: row.CANTIDAD, PRECIO: row.PRECIO, PESO_BRUTO: row.PESO_BRUTO, PESO_NETO: row.PESO_NETO, FLETES: row.FLETES, FOB: row.FOB }})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
        """
    tx.run(registry_query, file_path=fact_path)


def load_transactionType(tx):
    transactionType_path = "file:///DimTransactionType.csv"
    transactionType_query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{transactionType_path}" AS row
        RETURN row',
        'CREATE (:TransactionType {{CDTRANSACCION: row.CDTRANSACCION, DSTRANSACCION: row.DSTRANSACCION}})',

        {{batchSize: 2000, parallel: false, iterateList: true}}
    )
        """
    tx.run(transactionType_query, file_path=transactionType_path)


def drop_all(tx):
    query = """
   CALL apoc.periodic.iterate(
    "MATCH (n) RETURN n",
    "DETACH DELETE n",
    {batchSize: 5000, parallel: 'false', iterateList: 'true'}
)
    """
    # query="""
    # MATCH (n)
    # CALL {
    #     WITH n
    #     DETACH DELETE n
    # } IN TRANSACTIONS
    # """
    tx.run(query)


def create_facts_status_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (status:Status)
        WHERE fact.EstadoID = status.CDESTADO
        RETURN fact, status",

        "MERGE (fact)-[:estado]->(status)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_product_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (product:Product)
        WHERE fact.ProductoID = product.ProductID
        RETURN fact, product",

        "MERGE (fact)-[:producto]->(product)",

        {batchSize:2000, parallel:false, iterateList: true}
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

        "MERGE (fact)-[:Company]->(empresa)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_country_origin_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (country:Country)
        WHERE fact.PaisOrigenID = country.CountryID
        RETURN fact, country",

        "MERGE (fact)-[:PaisOrigen]->(country)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_country_flag_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (country:Country)
        WHERE fact.PaisBanderaID = country.CountryID
        RETURN fact, country",

        "MERGE (fact)-[:PaisBandera]->(country)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """

    tx.run(query)


def create_country_destiny_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (country:Country)
        WHERE fact.PaisDestinoID = country.CountryID
        RETURN fact, country",

        "MERGE (fact)-[:PaisDestino]->(country)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_country_procedence_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (country:Country)
        WHERE fact.PaisProcedenciaID = country.CountryID
        RETURN fact, country",

        "MERGE (fact)-[:PaisProcedencia]->(country)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)

def create_country_purchase_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (country:Country)
        WHERE fact.PaisCompraID = country.CountryID
        RETURN fact, country",

        "MERGE (fact)-[:PaisCompra]->(country)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_importers_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (importador:Importer)
        WHERE fact.ImporterId = importador.ImporterID
        RETURN fact, importador",

        "MERGE (fact)-[:Importador]->(importador)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_sia_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (sia:Sia)
        WHERE fact.SiaID = sia.siaID
        RETURN fact, sia",

        "MERGE (fact)-[:sia]->(sia)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_transaction_type_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (transaction:TransactionType)
        WHERE fact.TipoTransaccionID = transaction.CDTRANSACCION
        RETURN fact, transaction",

        "MERGE (fact)-[:TipoTransaccion]->(transaction)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_date_revision_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (date:Date)
        WHERE fact.FechaRevisionID = date.DateId
        RETURN fact, date",

        "MERGE (fact)-[:FechaRevision]->(date)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_date_rejection_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (date:Date)
        WHERE fact.FechaRechazoID = date.DateId
        RETURN fact, date",

        "MERGE (fact)-[:FechaRechazo]->(date)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_date_approval_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (date:Date)
        WHERE fact.FechaAprobacionID = date.DateId
        RETURN fact, date",

        "MERGE (fact)-[:FechaAprobacion]->(date)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_date_digitalization_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (date:Date)
        WHERE fact.FechaDigitalizacionID = date.DateId
        RETURN fact, date",

        "MERGE (fact)-[:FechaDigitalizacion]->(date)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)


def create_date_definitive_relationship(tx):
    query = """
    CALL apoc.periodic.iterate(
        "MATCH (fact:Facts)
        MATCH (date:Date)
        WHERE fact.FechaDefinitivoID = date.DateId
        RETURN fact, date",

        "MERGE (fact)-[:FechaDefinitivo]->(date)",

        {batchSize:2000, parallel:false, iterateList: true}
    )
    """
    tx.run(query)



with driver.session() as session:
    print("drop_all")
    session.execute_write(drop_all)
    print("load_company")
    session.execute_write(load_company)
    print("load_country")
    session.execute_write(load_country)
    print("load_date")
    session.execute_write(load_date)
    print("load_facts")
    session.execute_write(load_facts)
    print("load_importer")
    session.execute_write(load_importer)
    print("load_product")
    session.execute_write(load_product)
    print("load_status")
    session.execute_write(load_status)
    print("load_sia")
    session.execute_write(load_sia)
    print("load_transactionType")
    session.execute_write(load_transactionType)

    print("create_facts_status_relationship")
    session.execute_write(create_facts_status_relationship)
    print("create_product_relationship")
    session.execute_write(create_product_relationship)
    print("create_empresa_relationship")
    session.execute_write(create_empresa_relationship)

    print("create_country_purchase_relationship")
    session.execute_write(create_country_purchase_relationship)

    print("create_country_destiny_relationship")
    session.execute_write(create_country_destiny_relationship)
    print("create_country_flag_relationship")
    session.execute_write(create_country_flag_relationship)
    print("create_country_origin_relationship")
    session.execute_write(create_country_origin_relationship)
    print("create_country_procedence_relationship")
    session.execute_write(create_country_procedence_relationship)
    print("create_importers_relationship")
    session.execute_write(create_importers_relationship)
    print("create_sia_relationship")
    session.execute_write(create_sia_relationship)
    print("create_transaction_type_relationship")
    session.execute_write(create_transaction_type_relationship)
    print("create_date_approval_relationship")
    session.execute_write(create_date_approval_relationship)
    print("create_date_definitive_relationship")
    session.execute_write(create_date_definitive_relationship)
    print("create_date_digitalization_relationship")
    session.execute_write(create_date_digitalization_relationship)
    print("create_date_rejection_relationship")
    session.execute_write(create_date_rejection_relationship)
    print("create_date_revision_relationship")
    session.execute_write(create_date_revision_relationship)
