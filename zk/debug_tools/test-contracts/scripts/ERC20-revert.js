// deploys contracts and calls a method to produce delegate call
async function main() {
try {
   const ERC20 = await hre.ethers.getContractFactory("MyToken");

   // Deploy the contracts
   const erc20 = await ERC20.deploy();

   // Wait for the deployment transactions to be mined
   await erc20.waitForDeployment();

   console.log(`erc20 deployed to: ${await erc20.getAddress()}`);

   const transferFromResult = await erc20.transferFrom("0x5294ef2a2519BedC457D75b8FE5b132CCE32036a", "0xb218d64f29e5768B5Cc9b791B5514a2C611eE547", 1, {gasLimit: 1000000});
   console.log('transferFromResult method call transaction: ', transferFromResult.hash);
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