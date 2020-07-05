from os.path import abspath, dirname
import pandas as pd
import glob
import pytz as tz
from datetime import datetime

root_dir = "pfactors"
path = abspath(__file__)
_here = path.split(root_dir)[0] + root_dir

"""
NOTE : 

Timestamp is converted in 'Etc/GMT+7'

"""


class Parser(object):
    def __init__(self, pathname: str):
        self.file = self.get_files(pathname)
        self.file_name = None
        self.data = None

    def get_files(self, pattern: str) -> list:
        """
        Returns the list of files present in the given directory and pattern
        """
        return glob.glob(pattern)

    def get_file_path(self, index):
        return self.file[index]

    def get_file_name(self, path):
        self.file_name = path.split("/").pop()
        return self.file_name.split(".csv")[0]

    def get_all_data_from(self, pathname: str):
        """
        Returns the DataFrame object of the given file path : pathname
        """
        self.data = pd.read_csv(pathname)
        return self.data

    def get_index_of_value(self, data, header, value) -> int:
        indices = data[data[header] == value].index.values
        return int(indices) if len(indices) > 0 else Exception("There are no indices found in this given value.")

    def get_row_indices(self, data):
        return data.index.values


if __name__ == '__main__':
    input_files = Parser(_here + "/data/input/*.csv")
    dic_file = Parser(_here + "/resources/dictionary.csv")
    output_file_path = _here + "/data/output/"

    timestamp_format = "%m/%d/%Y %H:%M:%S"

    dic_file_data = dic_file.get_all_data_from(dic_file.get_file_path(0))
    i_power = dic_file.get_index_of_value(dic_file_data, 'key', 'Power')
    i_volts = dic_file.get_index_of_value(dic_file_data, 'key', 'Volts')
    i_energy = dic_file.get_index_of_value(dic_file_data, 'key', 'Energy')
    i_frequency = dic_file.get_index_of_value(dic_file_data, 'key', 'Frequency')
    i_temperature = dic_file.get_index_of_value(dic_file_data, 'key', 'Temperature')
    i_solar_a = dic_file.get_index_of_value(dic_file_data, 'key', 'SolarA')
    i_inverter_1 = dic_file.get_index_of_value(dic_file_data, 'key', 'Inverter1')
    i_inverter_2 = dic_file.get_index_of_value(dic_file_data, 'key', 'Inverter2')
    i_inverter_3 = dic_file.get_index_of_value(dic_file_data, 'key', 'Inverter3')

    main_df = pd.DataFrame(columns=['tag', 'time', 'value'])

    df_power = pd.DataFrame(columns=['tag', 'time', 'value'])
    df_volts = pd.DataFrame(columns=['tag', 'time', 'value'])
    df_energy = pd.DataFrame(columns=['tag', 'time', 'value'])
    df_frequency = pd.DataFrame(columns=['tag', 'time', 'value'])
    df_temperature = pd.DataFrame(columns=['tag', 'time', 'value'])

    for file in input_files.file:

        file_name = input_files.get_file_name(file)

        input_file_data = input_files.get_all_data_from(file)

        for i in input_file_data.index.values:
            site = input_file_data.loc[i, 'Site']

            if site == dic_file_data.loc[i_solar_a, 'key']:
                site = dic_file_data.loc[i_solar_a, 'value']

            device = input_file_data.loc[i, 'Device']

            if device == dic_file_data.loc[i_inverter_1, 'key']:
                device = dic_file_data.loc[i_inverter_1, 'value']
            elif device == dic_file_data.loc[i_inverter_2, 'key']:
                device = dic_file_data.loc[i_inverter_2, 'value']
            elif device == dic_file_data.loc[i_inverter_3, 'key']:
                device = dic_file_data.loc[i_inverter_3, 'value']

            _timestamp = input_file_data.loc[i, 'Date'] + " " + input_file_data.loc[i, 'Time']

            # print(timestamp)

            datetime_obj = datetime.strptime(_timestamp, timestamp_format)

            converted = datetime_obj.astimezone(tz.timezone('Etc/GMT+7'))

            timestamp = converted.strftime(timestamp_format)

            power = dic_file_data.loc[i_power, 'value']
            power_value = input_file_data.loc[i, 'Power']

            power_value = power_value / 1000

            df_power.loc[i, 'tag'] = "{site}.{device}.{col}".format(site=site, device=device, col=power)
            df_power.loc[i, 'time'] = timestamp
            df_power.loc[i, 'value'] = power_value

            volts = dic_file_data.loc[i_volts, 'value']
            volts_value = input_file_data.loc[i, 'Volts']

            df_volts.loc[i, 'tag'] = "{site}.{device}.{col}".format(site=site, device=device, col=volts)
            df_volts.loc[i, 'time'] = timestamp
            df_volts.loc[i, 'value'] = volts_value

            energy = dic_file_data.loc[i_energy, 'value']
            energy_value = input_file_data.loc[i, 'Energy']

            energy_value = energy_value / 1000

            df_energy.loc[i, 'tag'] = "{site}.{device}.{col}".format(site=site, device=device, col=energy)
            df_energy.loc[i, 'time'] = timestamp
            df_energy.loc[i, 'value'] = energy_value

            frequency = dic_file_data.loc[i_frequency, 'value']
            frequency_value = input_file_data.loc[i, 'Frequency']

            df_frequency.loc[i, 'tag'] = "{site}.{device}.{col}".format(site=site, device=device, col=frequency)
            df_frequency.loc[i, 'time'] = timestamp
            df_frequency.loc[i, 'value'] = frequency_value

            temperature = dic_file_data.loc[i_temperature, 'value']
            temperature_value = input_file_data.loc[i, 'Temperature']

            df_temperature.loc[i, 'tag'] = "{site}.{device}.{col}".format(site=site, device=device, col=temperature)
            df_temperature.loc[i, 'time'] = timestamp
            df_temperature.loc[i, 'value'] = temperature_value

        data = pd.concat([df_power, df_volts, df_energy, df_frequency, df_temperature])

        try:
            data.to_csv(output_file_path + "/{file_name}_parsed.csv".format(file_name=file_name), index=False, header=False)
            print("{file_name}_parsed.csv is successfully created in {output_file_path}".format(file_name=file_name, output_file_path=output_file_path))
        except Exception as e:
            print(e)
