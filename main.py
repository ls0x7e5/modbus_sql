# # setup logging
# import logging
# logging.basicConfig()
# log = logging.getLogger()
# log.setLevel(logging.DEBUG)

# PyModbus dependancies
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.client.sync import ModbusTcpClient

# posycopg2 dependencies
import psycopg2

# time dependancies
import time

# Get holding register data function
def read_holding_registers(ip_address, offset, node, length):
        """
        Retrieve modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.read_holding_registers(
            address=offset,  
            count=length,     
            unit=node)
    
        client.close()

        return response.registers

# Get input register data function
def read_input_registers(ip_address, offset, node, length):
        """
        Retrieve modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.read_input_registers(
            address=offset,  
            count=length,     
            unit=node)
    
        client.close()

        return response.registers

# Get input contact data function
def read_input_contacts(ip_address, offset, node, length):
        """
        Retrieve modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.read_discrete_inputs(
            address=offset,  
            count=length,     
            unit=node)

        client.close()

        return response.bits

# Get coil data function
def read_coils(ip_address, offset, node, length):
        """
        Retrieve modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.read_coils(
            address=offset,  
            count=length,     
            unit=node)

        client.close()

        return response.bits

# write holding register data function
# currently only supports 16bit integers
# BinaryPayloadBuilder() must be used to format other data types 
def write_holding_registers(ip_address, offset, node, data):
        """
        Write modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.write_registers(
            address=offset,  
            values=data,     
            unit=node)

        client.close()

        return response

# write coil data function
# currently only supports bits
# BinaryPayloadBuilder() must be used to format other data types 
def write_coils(ip_address, offset, node, data):
        """
        Write modbus data.
        """
        client = ModbusTcpClient(ip_address, timeout=10)
        client.connect()

        # Read registers
        response = client.write_coils(
            address=offset,  
            values=data,     
            unit=node)

        client.close()

        return response


def decode_binary( input_payload, data_type, byte_order, word_order, num_bytes ):
        """
        Decode modbus data.
        """

        decoded_val = BinaryPayloadDecoder.fromRegisters(
            registers=input_payload,
            byteorder=byte_order,
            wordorder=word_order)

        if data_type == "int8":
            output = [decoded_val.decode_8bit_int() for x in range(num_bytes)]
        elif data_type == "int16":
            output = [decoded_val.decode_16bit_int() for x in range(num_bytes//2)]
        elif data_type == "int32":
            output = [decoded_val.decode_32bit_int() for x in range(num_bytes//4)]
        elif data_type == "float32":
            output = [decoded_val.decode_32bit_float() for x in range(num_bytes//4)]
        else:
            output = [decoded_val.decode_16bit_int() for x in range(num_bytes//2)]

        return output

# Connect to the PostgreSQL database and write an entry to the table
# database name "b_database"
# user = "postgres"
# password = "password"
# db location = LOCALHOST
# table name = "table1"
# 4 rows

def write_to_database(data):
    try:
        # connect to database
        conn = psycopg2.connect(dbname="b_database", user="postgres", password="password")

        # creat cursor object
        cur = conn.cursor()

        # build statment
        sql = 'INSERT INTO table1 VALUES (%s, %s, %s, %s, %s)'

        # insert statment
        cur.execute(sql, (*data,))

        # commit the changes
        conn.commit()

        # close cursor object
        cur.close()

    # print any errors
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


''' @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ '''
''' @ ------------------------------------------------------------- @ '''
''' @ Program to poll PLC device and write to PostgreSQL database   @ '''
''' @ ------------------------------------------------------------- @ '''
''' @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ '''

# Setup

""" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% """
""" Enter the PLC modbus slave information """
""" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% """

# IP address(es)
slave_ip = ["192.168.1.5"]

# read/write coil offset(s)
# modbus addresses (0..1 - 0..9)
slave_rw_coil = [0]

# read input contact offset(s)  
# modbus addresses (1..1 - 1..9)
slave_read_contact = [0]

# read input register offset(s)
# modbus addresses (3..1 - 3..9)
slave_read_register = [0]

# read/write holding register offset(s)
# modbus addresses (4..1 - 4..9)
slave_rw_register = [1700]


# create the task scheduler and main task
# (Sleep after every iteration)
try:
    while True:

        # Send the modbus command to the plc
        response = read_holding_registers(slave_ip[0], slave_rw_register[0], 1, [0, 100])

        # decode the response if necessary
        val = decode_binary(response, "int32", Endian.Big, Endian.Big, 2*2)

        # print the decoded response
        print("PLC data: ")
        print(val)

        # write the response to the database, note static function parameters
        write_to_database(val)

        # set the log interval
        time.sleep(10)

except KeyboardInterrupt:
    print("Closing...")
    pass
