// deploys contracts and calls a method to produce delegate call
async function main() {
try {
   const DelegateCalled = await hre.ethers.getContractFactory("DelegateCalled");
   const DelegateCaller = await hre.ethers.getContractFactory("DelegateCaller");

   // Deploy the contracts
   const calledContract = await DelegateCalled.deploy();
   const callerContract = await DelegateCaller.deploy();

   // Wait for the deployment transactions to be mined
   await calledContract.waitForDeployment();
   await callerContract.waitForDeployment();

   console.log(`DelegateCalled deployed to: ${await calledContract.getAddress()}`);
   console.log(`DelegateCaller deployed to: ${await callerContract.getAddress()}`);

   const delegateCallResult = await callerContract.delegateCall(calledContract.getAddress(), 1);
   console.log('delegateCallResult method call transaction: ', delegateCallResult.hash);
 } catch (error) {
   console.error(error.toString());
   process.exit(1);
 }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });