async function main() {
try {
   // Get the ContractFactory of your SimpleContract
   const CounterContract = await hre.ethers.getContractFactory("Counter");

   // Deploy the contract
   const contract = await CounterContract.deploy({gasLimit: 140000, gasPrice: 1000000000});
   // Wait for the deployment transaction to be mined
   const deployResult = await contract.waitForDeployment();
   console.log(`Counter contract deployed to: ${await contract.getAddress()}`);

  //  const result = await contract.increment();
   console.log('Increment method call transaction: ', result.hash);
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