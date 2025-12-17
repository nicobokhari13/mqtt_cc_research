#from gpiozero import MCP3008


# import pigpio 
# pi = pigpio.pi()

# handle = pi.spi_open(0, 1000000)  # Open SPI device 0 with a baud rate of 1 MHz

# def read_adc(channel):
#     if channel < 0 or channel > 7:
#         return -1  # Invalid channel number
#     # SPI message format for MCP3008: [start bit, single-ended, channel number (3 bits), null bit (1 bit), data (10 bits)]
#     msg = [1, (8 + channel) << 4, 0]
#     data = pi.spi_xfer2(handle, msg)
#     adc_value = ((data[1] & 3) << 8) + data[2]  # Extract ADC value from received data
#     return adc_value


ADD_CONSTANT  = 0.6
CH_7_DIVISION_CONSTANT = (1.0526 + 0.9524)/2
CH_0_DIVISION_CONSTANT = (0.4524 + 0.5526)/2
STUNT_RESISTOR_OHMS = 1000

def readVoltage():
    channel0 ="30"# MCP3008(channel = 0)
    analogValue = 80.0#float(channel0.value)
    cZeroVoltage = analogValue  * 5 + ADD_CONSTANT
    voltage = cZeroVoltage / CH_0_DIVISION_CONSTANT
    print(f"voltage {voltage}")
    return voltage

def readCurrent():
    #channel7 = 40.0
    analogValue = 89.9#float(channel7.value)
    cSevenVoltage = analogValue * (5/1023) 
    voltage_drop = 3.3 - cSevenVoltage
    resistor = 220
    current = (voltage_drop / resistor) / CH_7_DIVISION_CONSTANT
    print(f"current {current}")
    return current