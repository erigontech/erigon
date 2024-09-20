
// deploys contracts and calls a method to produce delegate call

async function main() {
    const deployableBytecode = "608060405234801561000f575f80fd5b506101778061001d5f395ff3fe608060405234801561000f575f80fd5b506004361061003f575f3560e01c806306661abd14610043578063a87d942c14610061578063d09de08a1461007f575b5f80fd5b61004b610089565b60405161005891906100c8565b60405180910390f35b61006961008e565b60405161007691906100c8565b60405180910390f35b610087610096565b005b5f5481565b5f8054905090565b60015f808282546100a7919061010e565b92505081905550565b5f819050919050565b6100c2816100b0565b82525050565b5f6020820190506100db5f8301846100b9565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f610118826100b0565b9150610123836100b0565b925082820190508082111561013b5761013a6100e1565b5b9291505056fea2646970667358221220137ae5cf0fcdf694f11fbe24952b202d62e7154851f6232b7b897dbf37a2d18164736f6c63430008140033"
    try {
       const Creates = await hre.ethers.getContractFactory("Creates");
       
       // Deploy the contracts
       const createsContract = await Creates.deploy();
    
       // Wait for the deployment transactions to be mined
       await createsContract.waitForDeployment();
    
       console.log(`DelegateCalled deployed to: ${await createsContract.getAddress()}`);
    
       const opCreate = await createsContract.opCreate(hre.ethers.toUtf8Bytes(deployableBytecode), deployableBytecode.length);
       console.log('opCreate method call transaction: ', opCreate.hash);
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