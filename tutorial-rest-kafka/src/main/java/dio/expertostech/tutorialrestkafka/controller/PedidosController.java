package dio.expertostech.tutorialrestkafka.controller;

import dio.expertostech.tutorialrestkafka.data.PedidoData;
import dio.expertostech.tutorialrestkafka.service.RegistraEventoService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class PedidosController {

    private final RegistraEventoService eventoService;

    @PostMapping("/api/v1/salva-pedido")
    public ResponseEntity<String> salvarpedido(@RequestBody PedidoData pedido){
        eventoService.adicionarEvento("SalvarPedido", pedido);
        return ResponseEntity.ok("Sucesso");
    }
}
