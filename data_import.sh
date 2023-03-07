#!/bin/bash

echo importing csv file

echo importing DimCompany
mongoimport --db=Customs --collection=DimCompany --type=csv --headerline --file=./datamart_csv/DimCompany.csv --ignoreBlanks

echo importing DimCountry
mongoimport --db=Customs --collection=DimCountry --type=csv --headerline --file=./datamart_csv/DimCountry.csv --ignoreBlanks
echo importing DimDate
mongoimport --db=Customs --collection=DimDate --type=csv --headerline --file=./datamart_csv/DimDate.csv --ignoreBlanks
echo importing DimImporter
mongoimport --db=Customs --collection=DimImporter --type=csv --headerline --file=./datamart_csv/DimImporter.csv --ignoreBlanks
echo importing DimProduct
mongoimport --db=Customs --collection=DimProduct --type=csv --headerline --file=./datamart_csv/DimProduct.csv --ignoreBlanks
echo importing DimSia
mongoimport --db=Customs --collection=DimSia --type=csv --headerline --file=./datamart_csv/DimSia.csv --ignoreBlanks

echo importing DimStatus
mongoimport --db=Customs --collection=DimStatus --type=csv --headerline --file=./datamart_csv/DimStatus.csv --ignoreBlanks
echo importing FactImportRegistry
mongoimport --db=Customs --collection=FactImportRegistry --type=csv --headerline --file=./datamart_csv/FactImportRegistry.csv --ignoreBlanks

