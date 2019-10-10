# Transforms phenotype association files to rdf files
from rdflib import Graph, Literal, BNode, RDF
from rdflib.namespace import FOAF, DC, ClosedNamespace, RDFS
from rdflib.term import URIRef

import pandas as pd

import json

if __name__ == '__main__':

    PHENO = ClosedNamespace(
        uri=URIRef("http://phenodb.phenomebrowser.net/"),
        terms=[
            #Classes
            "Disease", "Drug", "Device", "Gene", "Genotype",
            "Phenotype", "Pathogen", "Provenance", "Association",

            #Properties
            "ecNumber", "uniprotId", "url", "failedToContributeToCondition"
        ]
    )

    OBO = ClosedNamespace(
        uri=URIRef("http://purl.obolibrary.org/obo/"),
        terms=[
            #has evidence
            "RO_0002558",
            #has phenotype
            "RO_0002200",
        ]
    )

    PUBCHEM = ClosedNamespace(uri=URIRef("https://pubchem.ncbi.nlm.nih.gov/compound/"), terms=[])
    MGI = ClosedNamespace(uri=URIRef("http://www.informatics.jax.org/marker/"), terms=[])
    ENTREZ_GENE = ClosedNamespace(uri=URIRef("https://www.ncbi.nlm.nih.gov/gene/"), terms=[])

    def create_graph():
        store = Graph()
        store.bind("dc", DC)
        store.bind("ddiem", PHENO)
        store.bind("obo", OBO)
        store.bind("pubchem", PUBCHEM)
        store.bind("mgi", MGI)
        store.bind("gene", ENTREZ_GENE)
        return store

    def transform_disease2phenotype():
        store = create_graph()
        filePath="data/DOID-Phenotypes-Formated.txt"
        df = pd.read_csv(filePath, sep='\t', names=['disease', 'phenotype']) 
        df.phenotype = df.phenotype.replace(regex=[':'], value='_')
        df.disease = df.disease.replace(regex=[':'], value='_')
        print(df.head())
        
        for index, row in df.iterrows():
            disease = store.resource(str(PHENO.uri) + row.disease)
            phenotype = store.resource(str(PHENO.uri) + row.phenotype)
            disease.add(OBO.RO_0002200, phenotype)
        

        store.serialize("data/disease2phenotype.rdf", format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_drug2phenotype():
        store = create_graph()
        filePath="data/Drug-phenotypes.txt"
        df = pd.read_csv(filePath, sep=' ', names=['drug', 'phenotype']) 
        df.phenotype = df.phenotype.replace(regex=['<'], value='').replace(regex=['>'], value='')
        df.drug = df.drug.replace(regex=['CID'], value='')
        print(df.head())
        
        for index, row in df.iterrows():
            drug = store.resource(str(PUBCHEM.uri) + row.drug)
            phenotype = store.resource(row.phenotype)
            drug.add(OBO.RO_0002200, phenotype)
        

        store.serialize("data/drug2phenotype.rdf", format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_gene2phenotype_text_mined():
        store = create_graph()
        filePath="data/merged.human.mouse.TM.extracts.expanded+NPMI.rank25.txt"
        df = pd.read_csv(filePath, sep='\t', names=['mgi', 'entrez_gene',  'phenotype', 'score']) 
        df.mgi = df.mgi.astype(str).replace(regex=['nan'], value='')
        df[['gene1', 'gene2']] = df.entrez_gene.str.split("_#_", expand = True)
        print(df.head())
        
        for index, row in df.iterrows():
            # print(row.mgi, row.phenotype, row.gene1, row.gene2)
            phenotype = store.resource(str(PHENO.uri) + row.phenotype)
            
            if row.mgi.strip():
                mgi = store.resource(str(MGI.uri) + row.mgi.strip())
                mgi.add(OBO.RO_0002200, phenotype)

            if row.gene1:
                gene = store.resource(str(ENTREZ_GENE.uri) + row.gene1.strip())
                gene.add(OBO.RO_0002200, phenotype)

            if row.gene2:
                gene = store.resource(str(ENTREZ_GENE.uri) + row.gene2.strip())
                gene.add(OBO.RO_0002200, phenotype)

        store.serialize("data/gene2phenotype_textmined.rdf", format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df
    
    def transform_pathogen2phenotype():
        store = create_graph()
        filePath="data/pathogens.4web.v3.txt"
        df = pd.read_json(filePath) 
        print(df.head())
        
        for index, row in df.iterrows():
            pathogen = store.resource(row.TaxID)
            for phenotype in row.Phenotypes:
                phenotypeRes = store.resource(phenotype['id'])
                pathogen.add(OBO.RO_0002200, phenotypeRes)
        

        store.serialize("data/pathogen2phenotype.rdf", format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    transform_disease2phenotype()
    transform_drug2phenotype()
    transform_gene2phenotype_text_mined()
    transform_pathogen2phenotype()