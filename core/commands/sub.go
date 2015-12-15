package commands

import (
	"fmt"
	cmds "github.com/ipfs/go-ipfs/commands"
	key "github.com/ipfs/go-ipfs/blocks/key"
	bitswap "github.com/ipfs/go-ipfs/exchange/bitswap"
	sublist "github.com/ipfs/go-ipfs/exchange/bitswap/sublist"
	u "github.com/ipfs/go-ipfs/util"
)

var SubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "TODO",
		ShortDescription: `
TODO.
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, true, "keys to subscribe").EnableStdin(),
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

		ts := []sublist.Topic{}
		for i, arg := range req.Arguments() {
			topic := sublist.Topic(arg)
fmt.Printf("SUB %v => %v, %v\n", i, arg, topic)
			ts = append(ts, topic)
		}

		keys := make(chan key.Key)
		defer close(keys)
		bs.SubTopics(ts, keys)

		for key := range keys {
			fmt.Printf(">>> %v\n", key)
		}
	},
}
