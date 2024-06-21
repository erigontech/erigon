// SPDX-License-Identifier: UNLICENSED
pragma solidity =0.8.19;

contract Counter {
    uint256 public number;

    function setNumber(uint256 newNumber) public {
        number = newNumber;
    }

    function increment() public returns(uint256) {
        number++;
        return number;
    }

    function tryRevert() public {
        number++;
        revert();
    }
    function terminate(address payable addr) public {
        selfdestruct(addr);
    }
    function stop() public {
        number++;
        assembly {stop()}
    }
}

contract Snowball {
    uint64 public seed;

    mapping(uint256 => uint256) public executionResults;
    mapping(uint256 => uint256) public primeNumbers;

    bytes returnData;
    bool success;

    uint256 public constant modeTx = 1;
    uint256 public constant modeMsg = 2;
    uint256 public constant modeBlock = 4;
    uint256 public constant modeContract = 8;
    uint256 public constant modePrecompile = 16;

    // LOG 0
    event TestStart() anonymous;

    // LOG 1
    event TestStartSeed(uint64 seed);

    // LOG 2
    event Test1(uint256 indexed h0);
    // LOG 3
    event Test2(uint256 indexed h0, uint256 indexed h1);
    // LOG 3
    event Test3(uint256 indexed h0, uint256 indexed h1, uint256 indexed h2);
    // LOG 3
    event Test4(uint256 indexed h0, uint256 indexed h1, uint256 indexed h2, uint256 h3);

    event PrimeCount(uint256 count);

    function calcPrimes(uint256 limit) public returns (uint256){
        bool[] memory primes = new bool[](limit + 1);

        // Initialize all numbers as prime
        for (uint256 i = 2; i <= limit; i++) {
            primes[i] = true;
        }

        for (uint256 p = 2; p * p <= limit; p++) {
            // If primes[p] is true, then it's a prime
            if (primes[p]) {
                // Mark all multiples of p as not prime
                for (uint256 i = p * p; i <= limit; i += p) {
                    primes[i] = false;
                }
            }
        }

        uint256 j = 0;
        for (uint256 i = 2; i <= limit; i++) {
            if (primes[i]) {
                primeNumbers[j] = i;
                j++;
            }
        }
        emit PrimeCount(j);
        return j;
    }

    // The goal of this function is to hit as many op codes as possible in a way that we can determine if any of that are behaving unexpectedly. There are a few op codes that we aren't testing
    // CALLCODE - deprecated for delegatecall
    // DUPX - We don't hit every dup call
    // MSTORE8
    // PC - Removed on solidity 0.7
    // PUSHX - we don't hit every variation
    // SHA3 - Also deprecated for kaccak256
    // SWAPX - we don't hit every variation
    //
    // We are also skipping a few procompiles at the moment
    // BLAKE2 - is not really used https://eips.ethereum.org/EIPS/eip-7266
    // ecParing - haven't figured out how to implement TODO
    function test(uint64 _seed, uint32 loops, uint256 mode) public payable returns (bytes32){
        seed = _seed;
        emit TestStartSeed(_seed);

        uint256 a;
        uint256 b;
        uint256 c;
        uint256 d;
        bytes32 snowball;

        uint256 h0;
        uint256 h1;
        uint256 h2;
        uint256 h3;

        emit TestStart();

        for (uint32 i = 0; i < loops; i = i + 1) {

            // ADD
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(a + b));

            // MUL
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a * b));

            // SUB
            a = generateNumber();
            b = generateNumber();
            unchecked {
                snowball = keccak256(abi.encodePacked(snowball, a - b));
            }

            // MSIZE
            assembly {a := msize()}
            snowball = keccak256(abi.encodePacked(snowball, a));

            // CODESIZE
            assembly {a := codesize()}
            snowball = keccak256(abi.encodePacked(snowball, a));

            // DIV
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a / b));

            // SDIV (CHECK)
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, int256(a) / (-1 * int256(b))));

            // MOD
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a % b));

            // SMOD (CHECK)
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, int256(a) % (-1 * int256(b))));

            // ADDMOD
            a = generateNumber();
            b = generateNumber();
            c = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, addmod(a,b, c)));

            // MULMOD
            a = generateNumber();
            b = generateNumber();
            c = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, mulmod(a,b, c)));

            // EXP
            a = generateNumber();
            b = generateNumber();
            unchecked {
                snowball = keccak256(abi.encodePacked(snowball, a ** b));
            }

            // SIGNEXTEND
            // get the random number, drop it to int64, then sign extend(?) to int256
            a = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, int256(int32(int256(a)))));

            // LT
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (a < b ? a : b)));

            // GT
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (a > b ? a : b)));

            // SLT
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (int256(a) < int256(b) ? a : b)));

            // SGT
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (int256(a) > int256(b) ? a : b)));

            // EQ
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (a == b ? a : b)));

            // ISZERO
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, (a == 0 ? a : b)));

            // AND
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a&b));

            // OR
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a|b));

            // XOR
            a = generateNumber();
            b = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a^b));

            // NOT
            a = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, ~a));

            // BYTE
            snowball = keccak256(abi.encodePacked(snowball, snowball[0]));

            // SHL
            a = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a << 1));

            // SHR
            a = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, a >> 1));

            // SAR
            a = generateNumber();
            snowball = keccak256(abi.encodePacked(snowball, int256(a) >> 1));


            // Tests after this point are going to vary randomly
            h0 = uint(snowball);

            // ADDRESS
            snowball = keccak256(abi.encodePacked(snowball, address(this)));
            snowball = keccak256(abi.encodePacked(snowball, address(this).code));
            snowball = keccak256(abi.encodePacked(snowball, address(this).codehash));

            // BALANCE
            // using addres(this).balance doesn't actually use the balance opcode?
            snowball = keccak256(abi.encodePacked(snowball, address(0).balance));
            snowball = keccak256(abi.encodePacked(snowball, address(this).balance));

            snowball = keccak256(abi.encodePacked(snowball, gasleft()));

            if (mode & modeTx == modeTx) {
                snowball = keccak256(abi.encodePacked(snowball, tx.origin));
                snowball = keccak256(abi.encodePacked(snowball, tx.gasprice));
            }
            h1 = uint(snowball);

            if (mode & modeMsg == modeMsg) {
                snowball = keccak256(abi.encodePacked(snowball, msg.sender));
                snowball = keccak256(abi.encodePacked(snowball, msg.data));
                snowball = keccak256(abi.encodePacked(snowball, msg.sig));
                snowball = keccak256(abi.encodePacked(snowball, msg.value));
            }
            h2 = uint(snowball);

            if (mode & modeBlock == modeBlock) {
                snowball = keccak256(abi.encodePacked(snowball, blockhash(block.number)));
                snowball = keccak256(abi.encodePacked(snowball, block.basefee));
                snowball = keccak256(abi.encodePacked(snowball, block.chainid));
                snowball = keccak256(abi.encodePacked(snowball, block.coinbase));
                snowball = keccak256(abi.encodePacked(snowball, block.difficulty));
                snowball = keccak256(abi.encodePacked(snowball, block.gaslimit));
                snowball = keccak256(abi.encodePacked(snowball, block.number));
                snowball = keccak256(abi.encodePacked(snowball, block.timestamp));
            }
            h3 = uint(snowball);

            if (mode & modeContract == modeContract) {
                a = generateNumber();
                Counter counter = new Counter();
                Counter counter2 = new Counter{salt: bytes32(a)}();
                counter2.terminate(payable(msg.sender));

                a = counter.increment();
                snowball = keccak256(abi.encodePacked(snowball, a));

                try counter.tryRevert() {
                } catch (bytes memory) {
                }

                snowball = keccak256(abi.encodePacked(snowball, address(counter).code));
                snowball = keccak256(abi.encodePacked(snowball, address(counter).codehash));

                (success, returnData) = address(counter).call{gas: 50000}(abi.encodeWithSignature("increment()"));
                snowball = keccak256(abi.encodePacked(snowball, success, returnData));

                (success, returnData) = address(counter).delegatecall{gas: 50000}(abi.encodeWithSignature("increment()"));
                snowball = keccak256(abi.encodePacked(snowball, success, returnData));

                (success, returnData) = address(counter).staticcall{gas: 50000}(abi.encodeWithSignature("increment()"));
                snowball = keccak256(abi.encodePacked(snowball, success, returnData));

                counter.stop();
            }

            if (mode & modePrecompile == modePrecompile) {
                // https://github.com/smartcontractkit/chainlink/blob/develop/contracts/src/v0.8/vrf/VRF.sol#L274
                a = generateNumber();
                b = generateNumber();
                c = generateNumber();
                address ec = ecrecover(bytes32(0), uint8(a), bytes32(b), bytes32(c));
                snowball = keccak256(abi.encodePacked(snowball, ec));

                a = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, sha256(abi.encodePacked(a))));

                a = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, ripemd160(abi.encodePacked(a))));

                a = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, callDatacopy(abi.encode(a))));

                // using an exponent of 24 to try to control gas
                a = generateNumber();
                b = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, modExp(a,24,b)));

                a = generateNumber();
                b = generateNumber();
                c = generateNumber();
                d = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, callBn256Add(bytes32(a),bytes32(b),bytes32(c),bytes32(d))));

                a = generateNumber();
                b = generateNumber();
                c = generateNumber();
                snowball = keccak256(abi.encodePacked(snowball, callBn256ScalarMul(bytes32(a),bytes32(b),bytes32(c))));

            }
        }

        emit Test1(h0);
        emit Test2(h0, h1);
        emit Test3(h0, h1, h2);
        emit Test4(h0, h1, h2, h3);

        executionResults[_seed] = uint(snowball);
        return snowball;
    }

    // copying the paramters from glibc for creating a sequence of pseudo-random numbers.
    // https://en.wikipedia.org/wiki/Linear_congruential_generator#Parameters_in_common_use
    function generateNumber() public returns (uint64) {
        seed = ((seed * 1103515245) + 12345) % 2147483648;
        return seed;
    }
    // https://docs.moonbeam.network/builders/pallets-precompiles/precompiles/eth-mainnet/#modular-exponentiation
    function modExp(uint256 _b, uint256 _e, uint256 _m) public returns (uint256 result) {
        assembly {
            // Free memory pointer
            let pointer := mload(0x40)
            // Define length of base, exponent and modulus. 0x20 == 32 bytes
            mstore(pointer, 0x20)
            mstore(add(pointer, 0x20), 0x20)
            mstore(add(pointer, 0x40), 0x20)
            // Define variables base, exponent and modulus
            mstore(add(pointer, 0x60), _b)
            mstore(add(pointer, 0x80), _e)
            mstore(add(pointer, 0xa0), _m)
            // Store the result
            let value := mload(0xc0)
            // Call the precompiled contract 0x05 = bigModExp
            if iszero(call(not(0), 0x05, 0, pointer, 0xc0, value, 0x20)) {
                revert(0, 0)
            }
            result := mload(value)
        }
    }

    function callBn256Add(bytes32 ax, bytes32 ay, bytes32 bx, bytes32 by) public returns (bytes32[2] memory result) {
        bytes32[4] memory input;
        input[0] = ax;
        input[1] = ay;
        input[2] = bx;
        input[3] = by;
        assembly {
            let succ := call(gas(), 0x06, 0, input, 0x80, result, 0x40)
            switch succ
            case 0 {
                revert(0,0)
            }
        }
    }
    function callBn256ScalarMul(bytes32 x, bytes32 y, bytes32 scalar) public returns (bytes32[2] memory result) {
        bytes32[3] memory input;
        input[0] = x;
        input[1] = y;
        input[2] = scalar;
        assembly {
            let succ := call(gas(), 0x07, 0, input, 0x60, result, 0x40)
            switch succ
            case 0 {
                revert(0,0)
            }
        }
    }
    function callBn256Pairing(bytes memory input) public returns (bytes32 result) {
        // input is a serialized bytes stream of (a1, b1, a2, b2, ..., ak, bk) from (G_1 x G_2)^k
        uint256 len = input.length;
        require(len % 192 == 0);
        assembly {
            let memPtr := mload(0x40)
            let succ := call(gas(), 0x08, 0, add(input, 0x20), len, memPtr, 0x20)
            switch succ
            case 0 {
                revert(0,0)
            } default {
                result := mload(memPtr)
            }
        }
    }

    function callDatacopy(bytes memory data) public returns (bytes memory) {
        bytes memory result = new bytes(data.length);
        assembly {
            let len := mload(data)
            if iszero(call(gas(), 0x04, 0, add(data, 0x20), len, add(result,0x20), len)) {
                invalid()
            }
        }
        return result;
    }

}
