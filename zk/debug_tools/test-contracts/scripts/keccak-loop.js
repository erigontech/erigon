async function main() {
try {
   // Get the ContractFactory of your KeccakLoopContract
   const KeccakLoopContract = await hre.ethers.getContractFactory("KeccakLoop");

   // Deploy the contract
   const contract = await KeccakLoopContract.deploy();
   // Wait for the deployment transaction to be mined
   await contract.waitForDeployment();

   console.log(`KeccakLoop deployed to: ${await contract.getAddress()}`);

  //  const result = await contract.bigLoop(10000);
  //  console.log(result);
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