import configparser
import os


###### lee el archivo de configuracion donde se encuentran parametros de la bd y paths entre otras cosas.
def readIni():
    

    config = configparser.ConfigParser()
    config.sections()

    
    
    #Obtén el directorio actual
    current_dir = os.getcwd()
    # Retrocede dos niveles
    #parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    #grandparent_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))

    # Construye la ruta al archivo deseado
    #archivo_deseado = os.path.join(grandparent_dir, 'configuration.ini')
    archivo_deseado = os.path.join(current_dir, 'configuration.ini')

    # Abre el archivo
    #with open(archivo_deseado, 'r') as file:
    #    contenido = file.read()

    # Haz algo con el contenido del archivo
    #print(contenido)
    config.read(archivo_deseado)


    #config.read('D:/ProcesosPython/configuration.ini')
    #config.read('D:/procesosPython/Proj_TemplateDataPipeline2/configuration.ini')

    
    return config

#if __name__ == "__main__":
    #readIni()   
