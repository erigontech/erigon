pragma solidity >=0.6.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o cmd/rpcdaemon/commands/contracts/build cmd/pics/contracts/token.sol
// ./build/bin/abigen -abi cmd/rpcdaemon/commands/contracts/build/Token.abi -bin cmd/rpcdaemon/commands/contracts/build/Token.bin -pkg contracts -type token -out cmd/rpcdaemon/commands/contracts/gen_token.go
contract Token {
    uint256 public totalSupply;
    mapping(address => uint256) public balanceOf;
    address public minter;

    constructor(address _minter) public {
        minter = _minter;
    }

    /* Send tokens */
    function transfer(address _to, uint256 _value) public returns (bool) {
        uint256 fromBalance = balanceOf[msg.sender];
        uint256 toBalance = balanceOf[_to];
        require(fromBalance >= _value);
        // Check if the sender has enough
        require(toBalance + _value >= toBalance);
        // Check for overflows
        balanceOf[msg.sender] = fromBalance - _value;
        // Subtract from the sender
        balanceOf[_to] = toBalance + _value;
        return true;
    }

    /* Allows the owner to mint more tokens */
    function mint(address _to, uint256 _value) public returns (bool) {
        require(msg.sender == minter);
        // Only the minter is allowed to mint
        uint256 toBalance = balanceOf[_to];
        require(toBalance + _value >= toBalance);
        // Check for overflows
        balanceOf[_to] = toBalance + _value;
        totalSupply += _value;
        return true;
    }
}
