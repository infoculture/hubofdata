Russian Centrobank open data

Website: http://www.opengovdata.ru

##DESCRIPTION
Скрипты по преобразованию API Центробанка в Открытые данные

Описание API Центробанка здесь - http://www.cbr.ru/scripts/Root.asp?Prtid=WSR

##REQUIREMENTS

- python 2.5+
- Microsoft .NET 2.0+


##INSTALLATION

Все уже преобразованные данные находятся в директории src/refined
Это:
 - tables.csv   - Таблицы 
 - regions.csv  - Субъекты федерации 
 - indicators.csv - Индикаторы таблиц
 - values.csv - значения индикаторов для конкретных регионов
 
Также в директории src/raw - находятся необработанные данные на 
основе которых были получены csv файлы в refined

Для самостоятельного получения и преобразования данных необходимо 
выполнить следующие действия:
1. Извлечь справочник регионов с сайта Центробанка
- scripts/cbrexporter.exe regions > raw/regions.xml

2. Извлечь справочник таблиц с сайта Центробанка
- scripts/cbrexporter.exe tables > raw/tables.xml

3. Преобразовать XML справонички регионов и таблиц в CSV  
- python cbrconvert.py regions
- python cbrconvert.py tables

4. Извлечь описания индикаторов с сайта Центробанка
- call scripts/get_indicators.bat

5. Преобразовать XML описания индикаторов в CSV формат
- python cbrconvert.py indicators

6. Произвести выгрузку значений индикаторов с сайта Центробанка
- python cbrextract.py

7. Преобразовать значения индикаторов в CSV формат 
- python cbrconvert.py values

7. Объединить все значения индикаторов в один CSV файл 
- python cbrconvert.py mergevalues


##LIMITATIONS
В виду ограничений накладываемых API Центробанка выгрузка данных с их 
сайта полноценно работает только если делать это используя Microsoft.NET
или VB примеры с их сайта. 
По этой причине выгрузка идёт с помощью специальной утилиты cbrexporter 
исходные коды которой входят в проект.

Возможно эта утилита лёгко соберётся под Mono, однако пока она делалась 
и работает в Microsoft.NET
 

##LICENSE
Этот код и данные доступны под условиями Creative Commons Unported 3.0 license http://creativecommons.org/licenses/by/3.0/
This code and data available under Creative Commons Unported 3.0 license http://creativecommons.org/licenses/by/3.0/





