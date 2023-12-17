import streamlit as st
from aws.client import S3Client
from datasource.csv import CSVCollector
from contract.catalogo import Catalogo

st.title("Essa é uma página de portal de dados")

# st.file_uploader("Upload a file", type=("png", "jpg"))


# if st.button("Say hello"):
#     st.write("Why hello there")

aws_instancia = S3Client()
catalogo_de_produto = CSVCollector(Catalogo, aws_instancia, "C11:I211")
catalogo_de_produto.start()
