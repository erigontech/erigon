async function main() {
try {

  const provider = hre.ethers.provider;
  const signer = await provider.getSigner();
  const nonce = await signer.getNonce();
  const balance = await provider.getBalance(signer.address);
  console.log("Balance before: " + balance);

  for (let i = nonce+10000; i >= nonce; i--) {
    try {
      await signer.sendTransaction({ 
        to: "0xB6f9665E564c0ADdA517c698Ebe32BA6Feb5Da35",
        value: hre.ethers.parseEther("0.000000000000000001"),
        gasPrice: 1, //if allowGreeTransactions flag is not set, the minimum gasPrice is 1gWei
        nonce: i
      });
    } catch(e) {
      console.log(e.toString());
    }
  }
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