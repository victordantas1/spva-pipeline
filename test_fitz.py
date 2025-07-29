import sys
import fitz # PyMuPDF

print("--- Teste de Importação do Fitz ---")
try:
    print(f"Versão do Python: {sys.version}")
    print(f"Caminho do Executável: {sys.executable}")
    print("-" * 20)
    print(f"Localização do Módulo fitz: {fitz.__file__}")
    print(f"Versão do PyMuPDF: {fitz.__doc__}")
    print("-" * 20)
    print("Importação do fitz foi BEM-SUCEDIDA!")
except Exception as e:
    print(f"A importação FALHOU com o erro: {e}")

