#!/usr/bin/env python3
import asyncio
import logging
import json
import random
import time
from datetime import datetime
from pymodbus.server.async_io import StartAsyncTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.version import version
from pymodbus.transaction import ModbusRtuFramer, ModbusAsciiFramer, ModbusBinaryFramer
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/modbus.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Device profiles to simulate real industrial equipment
DEVICE_PROFILES = {
    "schneider_pm5300": {
        "name": "Schneider Electric PowerLogic PM5300",
        "vendor": "Schneider Electric",
        "product_code": "PM5300",
        "revision": "2.0.0",
        "vendor_url": "http://www.schneider-electric.com",
        "product_name": "PowerLogic PM5300 Power Meter",
        "model_name": "PM5300",
        "registers": {
            "voltage_l1": {"address": 3000, "value": 230.5, "variation": 5},
            "voltage_l2": {"address": 3002, "value": 231.2, "variation": 5},
            "voltage_l3": {"address": 3004, "value": 229.8, "variation": 5},
            "current_l1": {"address": 3010, "value": 45.3, "variation": 10},
            "current_l2": {"address": 3012, "value": 44.8, "variation": 10},
            "current_l3": {"address": 3014, "value": 46.1, "variation": 10},
            "power_factor": {"address": 3020, "value": 0.95, "variation": 0.05},
            "frequency": {"address": 3022, "value": 50.0, "variation": 0.1},
            "total_power": {"address": 3024, "value": 31500, "variation": 1000}
        }
    },
    "siemens_s7_1200": {
        "name": "Siemens SIMATIC S7-1200",
        "vendor": "Siemens AG",
        "product_code": "6ES7 214-1AG40-0XB0",
        "revision": "4.2.3",
        "vendor_url": "http://www.siemens.com",
        "product_name": "SIMATIC S7-1200 CPU 1214C",
        "model_name": "CPU 1214C DC/DC/DC",
        "registers": {
            "cpu_temp": {"address": 100, "value": 45, "variation": 5},
            "scan_cycle": {"address": 102, "value": 12, "variation": 3},
            "system_status": {"address": 104, "value": 1, "variation": 0},
            "input_status": {"address": 200, "value": 0xFF, "variation": 0},
            "output_status": {"address": 202, "value": 0xAA, "variation": 0},
            "analog_in_1": {"address": 300, "value": 2048, "variation": 100},
            "analog_in_2": {"address": 302, "value": 1856, "variation": 100},
            "counter_1": {"address": 400, "value": 12345, "variation": 1000},
            "timer_1": {"address": 402, "value": 5678, "variation": 100}
        }
    },
    "abb_acs580": {
        "name": "ABB ACS580 Variable Frequency Drive",
        "vendor": "ABB",
        "product_code": "ACS580-01-12A7-4",
        "revision": "1.10",
        "vendor_url": "http://www.abb.com",
        "product_name": "ACS580 General Purpose Drive",
        "model_name": "ACS580",
        "registers": {
            "motor_speed": {"address": 1000, "value": 1450, "variation": 50},
            "motor_current": {"address": 1002, "value": 8.5, "variation": 2},
            "motor_torque": {"address": 1004, "value": 85, "variation": 10},
            "dc_bus_voltage": {"address": 1006, "value": 540, "variation": 10},
            "heatsink_temp": {"address": 1008, "value": 55, "variation": 5},
            "run_time": {"address": 1010, "value": 8760, "variation": 0},
            "fault_code": {"address": 1012, "value": 0, "variation": 0},
            "setpoint": {"address": 1014, "value": 1500, "variation": 0},
            "acceleration": {"address": 1016, "value": 10, "variation": 0}
        }
    },
    "allen_bradley_micrologix": {
        "name": "Allen-Bradley MicroLogix 1400",
        "vendor": "Rockwell Automation",
        "product_code": "1766-L32BXB",
        "revision": "21.0",
        "vendor_url": "http://www.rockwellautomation.com",
        "product_name": "MicroLogix 1400 Programmable Controller",
        "model_name": "1766-L32BXB",
        "registers": {
            "plc_status": {"address": 2000, "value": 0x06, "variation": 0},
            "fault_status": {"address": 2002, "value": 0, "variation": 0},
            "input_word_0": {"address": 2100, "value": 0xFFFF, "variation": 0},
            "output_word_0": {"address": 2200, "value": 0x5555, "variation": 0},
            "timer_acc_0": {"address": 2300, "value": 450, "variation": 50},
            "counter_acc_0": {"address": 2400, "value": 1234, "variation": 100},
            "analog_input_0": {"address": 2500, "value": 16384, "variation": 500},
            "analog_output_0": {"address": 2600, "value": 8192, "variation": 200},
            "comm_status": {"address": 2700, "value": 1, "variation": 0}
        }
    }
}

class ModbusHoneypot:
    def __init__(self, profile_name="schneider_pm5300"):
        self.profile = DEVICE_PROFILES.get(profile_name, DEVICE_PROFILES["schneider_pm5300"])
        self.start_time = time.time()
        self.request_count = 0
        self.unique_clients = set()
        
    def log_request(self, client_addr, function_code, address, count=None, values=None):
        """Log Modbus requests in JSON format for dashboard integration"""
        self.request_count += 1
        self.unique_clients.add(client_addr[0])
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "client_ip": client_addr[0],
            "client_port": client_addr[1],
            "function_code": function_code,
            "address": address,
            "count": count,
            "values": values,
            "device_profile": self.profile["name"],
            "request_number": self.request_count
        }
        
        # Write to JSON log file
        with open('/logs/modbus_requests.json', 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
            
        logger.info(f"Request from {client_addr[0]}:{client_addr[1]} - FC:{function_code} ADDR:{address}")
        
    def create_datastore(self):
        """Create Modbus datastore with realistic register values"""
        # Initialize data blocks with zeros
        coils = ModbusSequentialDataBlock(0, [0]*10000)
        discrete_inputs = ModbusSequentialDataBlock(0, [0]*10000)
        holding_registers = ModbusSequentialDataBlock(0, [0]*10000)
        input_registers = ModbusSequentialDataBlock(0, [0]*10000)
        
        # Populate registers based on device profile
        for reg_name, reg_info in self.profile["registers"].items():
            addr = reg_info["address"]
            base_value = reg_info["value"]
            variation = reg_info["variation"]
            
            # Add realistic variation to values
            if variation > 0:
                if isinstance(base_value, float):
                    value = base_value + random.uniform(-variation, variation)
                else:
                    value = int(base_value + random.randint(-int(variation), int(variation)))
            else:
                value = base_value
                
            # Handle different data types
            if isinstance(value, float):
                # Convert float to 32-bit representation (2 registers)
                builder = BinaryPayloadBuilder(byteorder=Endian.Big)
                builder.add_32bit_float(value)
                payload = builder.to_registers()
                holding_registers.setValues(addr, payload)
                input_registers.setValues(addr, payload)
            else:
                # Integer values
                if value < 0:
                    value = value & 0xFFFF  # Convert to unsigned
                holding_registers.setValues(addr, [value])
                input_registers.setValues(addr, [value])
                
        # Add realistic coil patterns based on device type
        if "pm5300" in self.profile["name"].lower():
            # Power meter status bits
            coils.setValues(0, [1])  # Power on
            coils.setValues(1, [0])  # No alarm
            coils.setValues(2, [1])  # Communication OK
            coils.setValues(3, [0])  # No fault
            coils.setValues(10, [1, 1, 1])  # Phase present L1, L2, L3
            
        elif "s7" in self.profile["name"].lower():
            # PLC I/O simulation
            # Inputs: sensors, switches
            for i in range(16):
                discrete_inputs.setValues(i, [random.randint(0, 1)])
            # Outputs: actuators, indicators
            for i in range(16):
                coils.setValues(i, [random.randint(0, 1)])
            # System flags
            coils.setValues(100, [1])  # RUN mode
            coils.setValues(101, [0])  # No force
            coils.setValues(102, [0])  # No error
            
        elif "acs580" in self.profile["name"].lower():
            # VFD status bits
            coils.setValues(0, [1])  # Ready
            coils.setValues(1, [1])  # Running
            coils.setValues(2, [0])  # No fault
            coils.setValues(3, [0])  # Not at speed
            coils.setValues(4, [0])  # No warning
            discrete_inputs.setValues(0, [1])  # Start command
            discrete_inputs.setValues(1, [0])  # Stop command
            discrete_inputs.setValues(2, [0])  # Reset
            
        elif "micrologix" in self.profile["name"].lower():
            # Allen-Bradley I/O patterns
            # Simulate ladder logic outputs
            for i in range(32):
                state = 1 if (i % 3) == 0 else 0
                coils.setValues(i, [state])
            # Input states
            for i in range(32):
                discrete_inputs.setValues(i, [random.randint(0, 1)])
            
        slave = ModbusSlaveContext(
            di=discrete_inputs,
            co=coils,
            hr=holding_registers,
            ir=input_registers
        )
        
        context = ModbusServerContext(slaves=slave, single=True)
        return context
        
    def get_device_identification(self):
        """Create device identification based on profile"""
        identity = ModbusDeviceIdentification()
        identity.VendorName = self.profile["vendor"]
        identity.ProductCode = self.profile["product_code"]
        identity.VendorUrl = self.profile["vendor_url"]
        identity.ProductName = self.profile["product_name"]
        identity.ModelName = self.profile["model_name"]
        identity.MajorMinorRevision = self.profile["revision"]
        
        # Add realistic serial numbers and manufacturing info
        serial_base = {
            "schneider_pm5300": "PM53",
            "siemens_s7_1200": "S7C",
            "abb_acs580": "ACS5",
            "allen_bradley_micrologix": "ML14"
        }
        base = serial_base.get(self.profile.get("name", "").replace(" ", "_").lower(), "DEV")
        identity.SerialNumber = f"{base}{random.randint(10000000, 99999999)}"
        identity.ManufacturerDate = f"{random.randint(2018, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        return identity

class RealisticModbusServer:
    """Implements realistic Modbus behavior including errors and timing variations"""
    
    @staticmethod
    def get_realistic_delay(function_code, register_count=1):
        """Calculate realistic response delay based on operation type"""
        # Base processing time varies by device
        base_delays = {
            1: 0.008,   # Read coils
            2: 0.008,   # Read discrete inputs
            3: 0.010,   # Read holding registers
            4: 0.010,   # Read input registers
            5: 0.015,   # Write single coil
            6: 0.015,   # Write single register
            15: 0.020,  # Write multiple coils
            16: 0.025,  # Write multiple registers
            17: 0.030,  # Report slave ID
            43: 0.040   # Read device identification
        }
        
        base = base_delays.get(function_code, 0.012)
        
        # Add time based on data size
        size_factor = register_count * 0.0002
        
        # Add realistic variance
        variance = random.gauss(0, base * 0.1)
        
        # Occasionally add network jitter (5% chance)
        if random.random() < 0.05:
            jitter = random.uniform(0.005, 0.050)
        else:
            jitter = 0
            
        total_delay = max(0.005, base + size_factor + variance + jitter)
        return total_delay
    
    @staticmethod
    def should_return_error():
        """Occasionally return realistic Modbus errors"""
        # Error probabilities (realistic for industrial networks)
        if random.random() < 0.002:  # 0.2% chance
            error_codes = [
                0x01,  # Illegal function
                0x02,  # Illegal data address
                0x03,  # Illegal data value
                0x04,  # Slave device failure
                0x06   # Slave device busy
            ]
            # Weighted selection (some errors more common)
            weights = [0.1, 0.3, 0.2, 0.1, 0.3]
            return random.choices(error_codes, weights=weights)[0]
        return None

async def run_modbus_server(profile_name="schneider_pm5300", port=502):
    """Run the Modbus TCP server"""
    honeypot = ModbusHoneypot(profile_name)
    
    # Log startup
    logger.info(f"Starting Modbus honeypot with profile: {profile_name}")
    logger.info(f"Device: {honeypot.profile['name']}")
    logger.info(f"Listening on port {port}")
    
    # Create datastore and identity
    context = honeypot.create_datastore()
    identity = honeypot.get_device_identification()
    
    # Custom request handler with realistic delays
    async def request_tracer(request, *args, **kwargs):
        if hasattr(request, 'address') and hasattr(request, 'function_code'):
            client_addr = kwargs.get('client_addr', ('unknown', 0))
            count = getattr(request, 'count', None)
            values = getattr(request, 'values', None)
            
            # Log the request
            honeypot.log_request(client_addr, request.function_code, request.address, count, values)
            
            # Add realistic delay
            delay = RealisticModbusServer.get_realistic_delay(
                request.function_code, 
                count if count else 1
            )
            await asyncio.sleep(delay)
            
            # Occasionally return errors
            error_code = RealisticModbusServer.should_return_error()
            if error_code:
                logger.info(f"Returning error code {error_code} to {client_addr[0]}")
                # In real implementation, would need to inject error response
    
    # Start register update task
    asyncio.create_task(update_registers_periodically(context, honeypot.profile, honeypot))
    
    # Start server
    server = await StartAsyncTcpServer(
        context=context,
        identity=identity,
        address=("0.0.0.0", port),
        allow_reuse_address=True,
        defer_start=False,
        custom_functions=[],
        request_tracer=request_tracer
    )
    
    return server

class ProcessSimulator:
    """Simulates realistic industrial processes with correlated values"""
    
    def __init__(self, profile_name):
        self.profile_name = profile_name
        self.time_offset = random.uniform(0, 24)  # Random start time for variation
        
    def get_time_factor(self):
        """Get time-based load factor (0.0 to 1.0)"""
        import math
        current_hour = (time.time() / 3600 + self.time_offset) % 24
        
        # Industrial load pattern (higher during work hours)
        if 6 <= current_hour <= 18:
            base_load = 0.7 + 0.2 * math.sin((current_hour - 6) * math.pi / 12)
        else:
            base_load = 0.3 + 0.1 * math.sin(current_hour * math.pi / 12)
            
        # Add some randomness
        return base_load + random.gauss(0, 0.05)
    
    def simulate_power_meter(self, registers):
        """Simulate realistic 3-phase power meter values"""
        load_factor = self.get_time_factor()
        
        # Grid voltage is relatively stable (±2%)
        base_voltage = 230
        voltage_variation = random.gauss(0, 2)
        
        # Current varies with load
        base_current = 100 * load_factor
        
        # Power factor degrades slightly with load
        power_factor = 0.98 - (0.05 * load_factor) + random.gauss(0, 0.01)
        power_factor = max(0.85, min(0.99, power_factor))
        
        # Calculate realistic 3-phase values with slight imbalance
        voltages = []
        currents = []
        for phase in range(3):
            v = base_voltage + voltage_variation + random.gauss(0, 0.5)
            i = base_current + random.gauss(0, base_current * 0.02)
            voltages.append(v)
            currents.append(i)
        
        # Total power calculation (3-phase)
        total_power = sum(v * i for v, i in zip(voltages, currents)) * power_factor / 1000
        
        # Frequency follows grid (very stable)
        frequency = 50.0 + random.gauss(0, 0.02)
        
        return {
            "voltage_l1": voltages[0],
            "voltage_l2": voltages[1],
            "voltage_l3": voltages[2],
            "current_l1": currents[0],
            "current_l2": currents[1],
            "current_l3": currents[2],
            "power_factor": power_factor,
            "frequency": frequency,
            "total_power": total_power
        }
    
    def simulate_plc(self, registers):
        """Simulate realistic PLC values"""
        load_factor = self.get_time_factor()
        
        # CPU temperature correlates with load
        base_temp = 35 + (20 * load_factor)
        cpu_temp = base_temp + random.gauss(0, 2)
        
        # Scan cycle varies with program complexity/load
        scan_cycle = 8 + (8 * load_factor) + random.gauss(0, 1)
        
        # System status (mostly OK, occasionally warnings)
        system_status = 1 if random.random() > 0.01 else 2
        
        # I/O simulation
        input_pattern = int(0xFF * load_factor) & 0xFF
        output_pattern = (~input_pattern) & 0xFF  # Inverse relationship
        
        # Analog values correlate with digital I/O
        analog_1 = int(4095 * load_factor) + random.randint(-100, 100)
        analog_2 = int(4095 * (1 - load_factor)) + random.randint(-100, 100)
        
        # Counters increment realistically
        current_counter = registers.get("counter_1", {}).get("value", 0)
        counter_increment = random.randint(0, int(10 * load_factor))
        
        return {
            "cpu_temp": int(cpu_temp),
            "scan_cycle": int(scan_cycle),
            "system_status": system_status,
            "input_status": input_pattern,
            "output_status": output_pattern,
            "analog_in_1": max(0, min(4095, analog_1)),
            "analog_in_2": max(0, min(4095, analog_2)),
            "counter_1": current_counter + counter_increment,
            "timer_1": int(time.time()) % 65536
        }
    
    def simulate_vfd(self, registers):
        """Simulate realistic Variable Frequency Drive"""
        load_factor = self.get_time_factor()
        setpoint = registers.get("setpoint", {}).get("value", 1500)
        
        # Motor speed ramps toward setpoint
        current_speed = registers.get("motor_speed", {}).get("value", 1450)
        ramp_rate = 50  # RPM per update
        
        if current_speed < setpoint:
            new_speed = min(current_speed + ramp_rate, setpoint)
        elif current_speed > setpoint:
            new_speed = max(current_speed - ramp_rate, setpoint)
        else:
            new_speed = current_speed + random.gauss(0, 5)
        
        # Current and torque correlate with speed and load
        motor_current = (new_speed / 1500) * 12 * load_factor + random.gauss(0, 0.5)
        motor_torque = (new_speed / 1500) * 100 * load_factor + random.gauss(0, 5)
        
        # DC bus voltage is relatively stable
        dc_bus = 540 + random.gauss(0, 5)
        
        # Heatsink temperature correlates with current
        heatsink_temp = 40 + (motor_current * 2) + random.gauss(0, 2)
        
        return {
            "motor_speed": int(new_speed),
            "motor_current": round(motor_current, 1),
            "motor_torque": int(motor_torque),
            "dc_bus_voltage": int(dc_bus),
            "heatsink_temp": int(heatsink_temp),
            "run_time": registers.get("run_time", {}).get("value", 8760) + 1,
            "fault_code": 0 if random.random() > 0.001 else random.choice([1, 2, 3]),
            "setpoint": setpoint,
            "acceleration": 10
        }
    
    def simulate_values(self, registers):
        """Main simulation dispatcher"""
        if "pm5300" in self.profile_name.lower():
            return self.simulate_power_meter(registers)
        elif "s7" in self.profile_name.lower():
            return self.simulate_plc(registers)
        elif "acs580" in self.profile_name.lower():
            return self.simulate_vfd(registers)
        elif "micrologix" in self.profile_name.lower():
            return self.simulate_plc(registers)
        else:
            return {}

async def update_registers_periodically(context, profile, honeypot):
    """Update register values periodically for realism"""
    simulator = ProcessSimulator(honeypot.profile["name"])
    
    while True:
        await asyncio.sleep(random.uniform(0.5, 2))  # Update every 0.5-2 seconds
        
        try:
            # Get current register values
            current_values = {}
            for reg_name, reg_info in profile["registers"].items():
                current_values[reg_name] = reg_info
            
            # Simulate new values
            new_values = simulator.simulate_values(current_values)
            
            # Update registers in context
            slave_context = context[0]  # Single slave mode
            
            for reg_name, new_value in new_values.items():
                if reg_name in profile["registers"]:
                    addr = profile["registers"][reg_name]["address"]
                    
                    if isinstance(new_value, float):
                        # Convert float to 32-bit representation (2 registers)
                        builder = BinaryPayloadBuilder(byteorder=Endian.Big)
                        builder.add_32bit_float(new_value)
                        payload = builder.to_registers()
                        slave_context.setValues(3, addr, payload)  # Function code 3 = holding registers
                        slave_context.setValues(4, addr, payload)  # Function code 4 = input registers
                    else:
                        # Integer values
                        if new_value < 0:
                            new_value = new_value & 0xFFFF
                        slave_context.setValues(3, addr, [new_value])
                        slave_context.setValues(4, addr, [new_value])
                        
        except Exception as e:
            logger.error(f"Error updating registers: {e}")
            continue

async def main():
    """Main entry point"""
    import os
    
    # Get configuration from environment
    profile = os.environ.get('MODBUS_PROFILE', 'schneider_pm5300')
    port = int(os.environ.get('MODBUS_PORT', '502'))
    
    # Validate profile
    if profile not in DEVICE_PROFILES:
        logger.warning(f"Unknown profile {profile}, using default")
        profile = 'schneider_pm5300'
    
    # Create required directories
    os.makedirs('/logs', exist_ok=True)
    
    # Start server
    server = await run_modbus_server(profile, port)
    
    # Keep running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())