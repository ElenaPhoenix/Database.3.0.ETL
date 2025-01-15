**Устанавливаем окружение**
Должен быть установлен Java.

https://spark.apache.org/downloads.html - разархивировать и положить в корневой каталог диска С
Пуск - Настройки - Система - О программе - Дополнительные параметры системы - Переменные среды - Переменные среды пользователя - Создать:
Имя HADOOP_HOME
Значение C:\hadoop
ок

Создать:
Имя SPARK_HOME
Значение C:\spark
ок

path - изменить (был C:\Program Files\MySQL\MySQL Shell 8.0\bin\) - создать:
%SPARK_HOME%\bin

создать:
%HADOOP_HOME%\bin
ок
ок
ок

Перезагрузка компьютера
Командная строка:
spark-shell

В spark можно зайти через браузер (путь прописан в командной строке)

Скачать и установить notepad ++ https://notepad-plus.plus/downloads/v8.6.8/
Скачать и установить heidi ssql https://www.heidisql.com/download.php