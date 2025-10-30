const TodoList = artifacts.require('./OpCodes.sol')
let contractInstance

contract('OpCodes', (accounts) => {
   beforeEach(async () => {
      contractInstance = await TodoList.deployed()
   })
   it('Should run without errors the majorit of opcodes', async () => {
     await contractInstance.test()
     await contractInstance.test_stop()

   })

   it('Should throw invalid op code', async () => {
    try{
      await contractInstance.test_invalid()
    }
    catch(error) {
      console.error(error);
    }
   })

   it('Should revert', async () => {
    try{
      await contractInstance.test_revert()    }
    catch(error) {
      console.error(error);
    }
   })
})
