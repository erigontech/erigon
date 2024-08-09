// deploys contracts and calls a method to produce delegate call
async function main() {
  try {
    const signer = await hre.ethers.provider.getSigner();

    const ChainCallLevel1 = await hre.ethers.getContractFactory("ChainCallLevel1");
    const ChainCallLevel2 = await hre.ethers.getContractFactory("ChainCallLevel2");
    const ChainCallLevel3 = await hre.ethers.getContractFactory("ChainCallLevel3");
    const ChainCallLevel4 = await hre.ethers.getContractFactory("ChainCallLevel4");

    console.log('Deploying contracts...');

    // Deploy the contracts
    const chainCallLevel1 = await ChainCallLevel1.deploy();
    const chainCallLevel2 = await ChainCallLevel2.deploy();
    const chainCallLevel3 = await ChainCallLevel3.deploy();
    const chainCallLevel4 = await ChainCallLevel4.deploy();

    // Wait for the deployment transactions to be mined
    const addresses = []
    await chainCallLevel1.waitForDeployment();
    addresses[0] = await chainCallLevel1.getAddress();
    console.log(`ChainCallLevel1 deployed to: ${addresses[0]}`);

    await chainCallLevel2.waitForDeployment();
    addresses[1] = await chainCallLevel2.getAddress();
    console.log(`ChainCallLevel2 deployed to: ${addresses[1]}`);

    await chainCallLevel3.waitForDeployment();
    addresses[2] = await chainCallLevel3.getAddress();
    console.log(`ChainCallLevel3 deployed to: ${addresses[2]}`);

    await chainCallLevel4.waitForDeployment();
    addresses[3] = await chainCallLevel4.getAddress();
    console.log(`ChainCallLevel4 deployed to: ${addresses[3]}`);

    console.log("Sending funds to the contracts...");
    const amountWei = 1;
    for (let i = 0; i < addresses.length; i++) {
      const send = await signer.sendTransaction({
        to: addresses[i],
        value: ethers.parseUnits(amountWei.toString(), 'wei'),
      });
      console.log(`Sent ${amountWei} wei to ${addresses[i]}. TxHash: ${send.hash}`);
    }


    const execCallResult = await chainCallLevel1.exec(addresses[1], addresses[2], addresses[3], {value: ethers.parseUnits(amountWei.toString(), 'wei')});
    console.log('exec method call transaction: ', execCallResult.hash);
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