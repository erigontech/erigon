async function main() {
try {
   // Get the ContractFactory of your SimpleContract
   const SimpleContract = await hre.ethers.getContractFactory("EmitLog");

   // Deploy the contract
   const contract = await SimpleContract.deploy();
   // Wait for the deployment transaction to be mined
   await contract.waitForDeployment();

   console.log(`EmitLog deployed to: ${await contract.getAddress()}`);

   const result = await contract.emitLogs();
   console.log('emitLogs method call transaction: ', result.hash);
 } catch (error) {
   console.error(error);
   process.exit(1);
 }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });