# template_meta
Template con el codigo para el export de meta via API

## Requisitos

- Contar con las librerías necesarias, que se pueden instalar utilizando el archivo `requirements.txt`.
- Sustituir el contenido del archivo `key.json.example` con las credenciales del proyecto de dimensiones de Google Cloud.
    - En proyectos antiguos se puede encontrar como `key.json`.
- Sustituir el contenido del archivo `proyecto.json.example` con las credenciales de su proyecto de Google Cloud.
    - En proyectos antiguos se puede encontrar como `<nombre_proyecto>.json`.
- En la tabla dimensiones.Data_Cruda.metaapi, crear un nuevo registro con la informacion de la cuenta de Meta y la aplicacion de Meta.
    - El campo `access_token` debe contener el token de acceso de la cuenta de Meta.
    - El campo `app_id` debe contener el ID de la aplicacion de Meta.
    - El campo `app_secret` debe contener el secreto de la aplicacion de Meta.
    - El campo `api_version` debe contener la version de la API de Meta que se va a utilizar (por ejemplo, 'v16.0').
    - El campo `cliente` debe contener el nombre del cliente, de forma que se pueda identificar en la tabla de dimensiones.
    - El campo `id` debe contener un identificador único respecto al resto de registros en la tabla. Y se va utilizar para obtener los datos de la cuenta de Meta que se encuentran en la tabla. 
- En el archivo `main.py`, dentro de la funcion `process_and_load_data`, se debe especificar el ID del registro de la tabla dimensiones.Data_Cruda.metaapi que se va a utilizar para obtener los datos de Meta, en la linea 246, dentro de la variable `ids`.
- `main.py` soporta multiples IDs, por lo que se puede obtener datos de varias cuentas de Meta en una sola ejecución, donde los datos finales se almacenan en un único DataFrame.
- En el archivo `main.py`, también se debe modificar dentro de la función `process_and_load_data` el nombre/id del proyecto y de la tabla donde se van a almacenar los datos. Hasta el momento de escritura, la tabla se nombra normalmente como `Data_Cruda.meta_api_std`.

## Otros comentarios

- `main.py` contiene la lógica para extraer distintas metricas varias de la api de meta, mas info en la [documentacion](https://developers.facebook.com/docs/marketing-api/reference/ads-action-stats/)
- Para ejecutar el script, se debe agregar un `if __name__ == "__main__":` al final del archivo `main.py` que llame a la función `process_and_load_data`. Luego se puede ejecutar el script directamente.