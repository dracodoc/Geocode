__author__ = 'draco'
# to generate shell script that batch geocoding intersections. Using bash directly involved passing parameters from bash to psql to script, too complicated now.
import os

# inputFolder = "d:\\Work\\Geocode\\address_input\\"
inputFolder = "/home/ubuntu/geocode/intersection_input/"
outputFolder = "/home/ubuntu/geocode/intersection_output/"
# assumption: log_intersection, geocode_intersection.sql, intersection_table

# psql -d census -U postgres -h localhost -w -v input_file="'/home/ubuntu/geocode/address_input/address.csv'"
# -f geocode_batch.sql 2>&1 | tee log/1.log
# psql -d census -U postgres -h localhost -w -c '\copy address_table to /home/ubuntu/geocode/address_output/1.csv csv header'

commands = []
for fileName in sorted(os.listdir(inputFolder)):
    if fileName.endswith(".csv"):
        comment = 'echo " ------ processing file: ' + fileName + '-------"'
        commands.append(comment)
        command_1 = 'psql -d census -U postgres -h localhost -w -v input_file="' + "'" + os.path.join(inputFolder, fileName) + "'" \
                  '" -v output_file="' + "'" + os.path.join(outputFolder, fileName) + "'"  \
                  + '" -f geocode_intersection.sql 2>&1 | tee log_intersection/' + fileName + '.log'
        commands.append(command_1)
        command_2 = "psql -d census -U postgres -h localhost -w -c '\copy intersection_table to " + \
                    os.path.join(outputFolder, 'output_' + fileName) + " csv header'"
        commands.append(command_2)

# print commands[0]

# script = open("f:\\batch.sh", "w")
script = open("batch.sh", "w")
script.write("\n".join(commands))