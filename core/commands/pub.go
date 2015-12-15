package commands

import (
	"fmt"
	key "github.com/ipfs/go-ipfs/blocks/key"
	cmds "github.com/ipfs/go-ipfs/commands"
	bitswap "github.com/ipfs/go-ipfs/exchange/bitswap"
	sublist "github.com/ipfs/go-ipfs/exchange/bitswap/sublist"
	publist "github.com/ipfs/go-ipfs/exchange/bitswap/publist"
	u "github.com/ipfs/go-ipfs/util"
)

var PubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "TODO",
		ShortDescription: `
TODO.
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, true, "key to TODO").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if !nd.OnlineMode() {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		bs, ok := nd.Exchange.(*bitswap.Bitswap)
		if !ok {
			res.SetError(u.ErrCast(), cmds.ErrNormal)
			return
		}

		topic := sublist.Topic(req.Arguments()[0])

		ps := []publist.Pub{}
		for i, arg := range req.Arguments()[1:] {
			key := key.B58KeyDecode(arg)
			if key == "" {
				res.SetError(fmt.Errorf("incorrectly formatted key: %s", arg), cmds.ErrNormal)
				return
			}
fmt.Printf("PUB %v => %v, %v\n", i, topic, key)
			ps = append(ps, publist.Pub{topic, key})
		}
		bs.PubPubs(ps)
	},
}
