# Transforms phenotype association files to rdf files
from rdflib import Graph, Literal, BNode, RDF
from rdflib.namespace import FOAF, DC, ClosedNamespace, RDFS, DCTERMS
from rdflib.term import URIRef

from pathlib import Path

import pandas as pd

import json
import uuid
import sys

if __name__ == '__main__':

    DATA_FOLDER = (sys.argv[1] if len(sys.argv) == 2 else str(Path.home()) + '/data/phenodb')
    print(DATA_FOLDER)

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
            #phenotypic similarity evidence used in automatic assertion
            "ECO_0007824",
            #curator inference used in manual assertion (manually curated)
            "ECO_0000305",
            #similarity evidence used in automatic assertion
            "ECO_0000251",
            #computational evidence used in automatic assertion (text mining, lexical matching, based on NPMI value)
            "ECO_0007669"
        ]
    )

    PUBCHEM = ClosedNamespace(uri=URIRef("https://pubchem.ncbi.nlm.nih.gov/compound/"), terms=[])
    MGI = ClosedNamespace(uri=URIRef("http://www.informatics.jax.org/marker/"), terms=[])
    ENTREZ_GENE = ClosedNamespace(uri=URIRef("https://www.ncbi.nlm.nih.gov/gene/"), terms=[])

    def create_graph():
        store = Graph()
        store.bind("dc", DC)
        store.bind("dcterms", DCTERMS)
        store.bind("pheno", PHENO)
        store.bind("obo", OBO)
        store.bind("pubchem", PUBCHEM)
        store.bind("mgi", MGI)
        store.bind("gene", ENTREZ_GENE)
        return store

    def add_association_provenance(store, association, creator=None, created_on=None, source=None):
        provenance = store.resource(str(PHENO.uri) + str(uuid.uuid4()))
        provenance.add(RDF.type, DCTERMS.ProvenanceStatement)
        if creator:
            provenance.add(DC.creator, Literal(creator))
        if created_on:
            provenance.add(DCTERMS.created, Literal(created_on))
        if source:
            provenance.add(DCTERMS.source, Literal(source))

        association.add(DC.provenance, provenance)
        return association
    
    def create_phenotypic_association(store, subject, object):
        association = store.resource(str(PHENO.uri) + str(uuid.uuid4()))
        association.add(RDF.type, RDF.Statement)
        association.add(RDF.subject, subject)
        association.add(RDF.predicate,OBO.RO_0002200)
        association.add(RDF.object, object)
        return association

    def transform_disease2phenotype():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/DOID-Phenotypes-Formated.txt'
        df = pd.read_csv(filePath, sep='\t', names=['disease', 'phenotype']) 
        df.phenotype = df.phenotype.replace(regex=[':'], value='_')
        df.disease = df.disease.replace(regex=[':'], value='_')
        print(df.head())
        
        for index, row in df.iterrows():
            disease = store.resource(str(OBO.uri) + row.disease)
            disease.add(RDF.type, PHENO.Disease)
            phenotype = store.resource(str(OBO.uri) + row.phenotype)
            phenotype.add(RDF.type, PHENO.Phenotype)
            association = create_phenotypic_association(store, disease, phenotype)
            association.add(OBO.RO_0002558, OBO.ECO_0007669)
            add_association_provenance(store, association, creator='Sara Althubaiti', created_on='2018-11-07')
        

        store.serialize(f'{DATA_FOLDER}/disease2phenotype.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_drug2phenotype():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/Drug-phenotypes.txt'
        df = pd.read_csv(filePath, sep=' ', names=['drug', 'phenotype']) 
        df.phenotype = df.phenotype.replace(regex=['<'], value='').replace(regex=['>'], value='')
        df.drug = df.drug.replace(regex=['CID'], value='')
        print(df.head())
        
        for index, row in df.iterrows():
            drug = store.resource(str(PUBCHEM.uri) + row.drug)
            drug.add(RDF.type, PHENO.Drug)
            phenotype = store.resource(row.phenotype)
            phenotype.add(RDF.type, PHENO.Phenotype)
            association = create_phenotypic_association(store, drug, phenotype)
            association.add(OBO.RO_0002558, OBO.ECO_0007669)
            add_association_provenance(store, association, creator='Sara Althubaiti', created_on='2019-03-12')

        store.serialize(f'{DATA_FOLDER}/drug2phenotype.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_gene2phenotype_text_mined():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/merged.human.mouse.TM.extracts.expanded+NPMI.rank25.txt'
        df = pd.read_csv(filePath, sep='\t', names=['mgi', 'entrez_gene',  'phenotype', 'score']) 
        df.mgi = df.mgi.astype(str).replace(regex=['nan'], value='')
        df[['gene1', 'gene2']] = df.entrez_gene.str.split("_#_", expand = True)
        print(df.head())
        
        for index, row in df.iterrows():
            # print(row.mgi, row.phenotype, row.gene1, row.gene2)
            phenotype = store.resource(str(OBO.uri) + row.phenotype)
            phenotype.add(RDF.type, PHENO.Phenotype)
            
            if row.mgi.strip():
                mgi = store.resource(str(MGI.uri) + row.mgi.strip())
                mgi.add(RDF.type, PHENO.Gene)
                association = create_phenotypic_association(store, mgi, phenotype)
                association.add(OBO.RO_0002558, OBO.ECO_0007669)
                add_association_provenance(store, association, creator='Senay Kafkas', 
                    created_on='2019-01-06', source='https://www.ncbi.nlm.nih.gov/pubmed/30809638')

            if row.gene1:
                gene = store.resource(str(ENTREZ_GENE.uri) + row.gene1.strip())
                gene.add(RDF.type, PHENO.Gene)
                association = create_phenotypic_association(store, gene, phenotype)
                association.add(OBO.RO_0002558, OBO.ECO_0007669)
                add_association_provenance(store, association, creator='Senay Kafkas', 
                    created_on='2019-01-06', source='https://www.ncbi.nlm.nih.gov/pubmed/30809638')

            if row.gene2:
                gene = store.resource(str(ENTREZ_GENE.uri) + row.gene2.strip())
                gene.add(RDF.type, PHENO.Gene)
                association = create_phenotypic_association(store, gene, phenotype)
                association.add(OBO.RO_0002558, OBO.ECO_0007669)
                add_association_provenance(store, association, creator='Senay Kafkas', 
                    created_on='2019-01-06', source='https://www.ncbi.nlm.nih.gov/pubmed/30809638')

        store.serialize(f'{DATA_FOLDER}/gene2phenotype_textmined.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df
    
    def transform_pathogen2phenotype():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/pathogens.4web.v3.txt'
        df = pd.read_json(filePath) 
        print(df.head())
        
        for index, row in df.iterrows():
            pathogen = store.resource(row.TaxID)
            pathogen.add(RDF.type, PHENO.Pathogen)
            evidences = []
            for method in row.Diseases[0]['method'].split(","):
                if not method.strip():
                    continue
                
                if "text mining" in method:
                    evidences.append(OBO.ECO_0007669)
                elif "manual curation" in method:
                    evidences.append(OBO.ECO_0000305)

            for phenotype in row.Phenotypes:
                phenotypeRes = store.resource(phenotype['id'])
                phenotypeRes.add(RDF.type, PHENO.Phenotype)
                association = create_phenotypic_association(store, pathogen, phenotypeRes)
                for evidence in evidences:
                    association.add(OBO.RO_0002558, evidence)

                add_association_provenance(store, association, creator='Senay Kafkas', 
                    created_on='2019-06-03', source='https://www.nature.com/articles/s41597-019-0090-x')

        store.serialize(f'{DATA_FOLDER}/pathogen2phenotype.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_mondo2phenotype_top50():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/mondo-pheno.pairs.top50.txt'
        df = pd.read_csv(filePath, sep='\t') 
        df.Phenotype_ID = df.Phenotype_ID.replace(regex=[':'], value='_')
        df.Mondo_ID = df.Mondo_ID.replace(regex=[':'], value='_')
        print(df.head())
        
        for index, row in df.iterrows():
            disease = store.resource(str(OBO.uri) + row.Mondo_ID)
            disease.add(RDF.type, PHENO.Disease)
            phenotype = store.resource(str(OBO.uri) + row.Phenotype_ID)
            phenotype.add(RDF.type, PHENO.Phenotype)
            association = create_phenotypic_association(store, disease, phenotype)
            association.add(OBO.RO_0002558, OBO.ECO_0007669)
            add_association_provenance(store, association, creator='Senay Kafkas')
    

        store.serialize(f'{DATA_FOLDER}/mondo2phenotype_top50.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df

    def transform_predictive_gene2phenotype():
        store = create_graph()
        filePath=f'{DATA_FOLDER}/gene_human_phenotypes_maxat.txt'
        df = pd.read_csv(filePath, sep='\t', names=['gene', 'phenotype', 'score']) 
        df.phenotype = df.phenotype.replace(regex=[':'], value='_')
        df.gene = df.gene.astype(str)
        print(df.head())
        
        split_count = 1
        for index, row in df.iterrows():
            gene = store.resource(str(ENTREZ_GENE.uri) + row.gene)
            gene.add(RDF.type, PHENO.Gene)
            phenotype = store.resource(str(OBO.uri) + row.phenotype)
            phenotype.add(RDF.type, PHENO.Phenotype)
            association = create_phenotypic_association(store, gene, phenotype)
            association.add(OBO.RO_0002558, OBO.ECO_0007669)
            add_association_provenance(store, association, creator='Maxat Kulmanov', created_on='2019-10-20')

            if index > 0 and index % 5_00_000 == 0:
                store.serialize(f'{DATA_FOLDER}/predictive_gene2phenotype-{split_count}.rdf', format="pretty-xml", max_depth=3)
                print(len(store))
                store.remove((None, None, None))
                split_count += 1

        split_count += 1
        store.serialize(f'{DATA_FOLDER}/predictive_gene2phenotype-{split_count}.rdf', format="pretty-xml", max_depth=3)
        print(len(store))
        store.remove((None, None, None))
        del df
    
    transform_disease2phenotype()
    transform_drug2phenotype()
    transform_gene2phenotype_text_mined()
    transform_pathogen2phenotype()
    transform_mondo2phenotype_top50()
    transform_predictive_gene2phenotype()