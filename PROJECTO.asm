BUFFER	EQU	4000	         ; endere�o de mem�ria onde se guarda a tecla
LINHA	EQU	8        ; posi��o do bit correspondente a linha (4) a testar
UNICAVEZ EQU 4002
PIN     EQU 0E000H           ;Endereço do porto de entrada do teclado
POUT    EQU 0C000H			 ;Endereço do porto de saida do teclado
;-----------------------------------------------------------------------------------
                        ;Constantes do processo do ecrã

pixelB2 EQU 4004            ;valor do endereço dos pixels da bala2
pixelB1 EQU 4010            ;valor dos endereços dos pixels da bala1


BaseVectorB1 EQU 4016       ;valor do endereço das direções do pixel da bala1
BaseVectorB2 EQU 4026       ;valor do endereço da direção do pixel da bala2

LocalizaPixelIn2 EQU 4036   ;valor dos endereços dos pixels do inimigo2
DescerIn1 EQU 4080          ;valor usado para direcionar o tanque inimigo para descer
LocalizaPixelIn1 EQU 4086   ;valor dos endereços dos pixels do inimigo1
conta EQU 4136              ;Valor usado para armazenar a ordem de criação dos inimg
ondePara equ 4142           ;Valor usado para armazenar onde ira parar a contagem
BaseVector2 EQU 4146        ;valor do endereço das direções do pixel do inimigo2
BaseVectorIn1 equ 4154      ;valor do endereço das direções do pixel do inimigo1

stackSize  EQU 100H         ;valor do tamanho da pilha

baseVectoresJ equ 4168      ;valor do endereço das direções do pixel do jogador
localizaPixelJ EQU 4178     ;valor dos endereços dos pixels do jogador
BaseTorpedo EQU 4208        ;valor do endereço das direções do pixel do torpedo
LocalizaPixelTorpedo EQU 4216 ;valor dos endereços dos pixels do torpedo
pixelsMatriz  EQU 8000H      ;inicio do endereço do ecrã
FizEsperarTeclado EQU 4250   ;valor do endereço usado para testar na chamada do teclado
;-----------------------------------------------------------------------------------
						;Constantes dos Display

displays 	EQU	0A000H	  ;endereço do porto dos displays hexadecimais
nibble_3_0	EQU	000FH     ;máscara para isolar os 4 bits de menor peso
nibble_7_4	EQU	00F0H	  ; máscara para isolar os bits 7 a 4	


VDisplayLH EQU 4232       ;Endereço dos display usado para armazenar valor em tempo

matei2 EQU 4240
QualLocalizacaoArmazenar EQU 4400
imagem_hexa: STRING	00H   ;imagem em memória dos displays hexadecimais inicializada a zero

PLACE 3000H
pilha: TABLE stackSize    ;Criação da pilha
baseDaPilha:
PLACE 3200H
ptable:STRING 01H,02H,04H,08H,10H,20H,40H,80H


PLACE 0
main: 	      
	      MOV SP,baseDaPilha    ;Inicialização do registro da pilha
              CALL RecomecoInimigo1 ;Inicialização das posições do tanque inimigo1
              CALL REcomecoInimigo2 ;Inicialização das posições do tanque inimigo2
              CALL RecomecoB1       ;Inicialização das posições da bala1
              CALL RecomecoB2       ;Inicialização das posições da bala2
			  CALL Carregamento	    ;preprocessamento
			  CALL torpedo          ;Cria o torpedo
;----------------------------------------------------------------------------
;Etiqueta responsável para chamar os processos tais como a criação do tanque do 
;jogador principal o processo do teclado  esta etiqueta so chama o processo de 
;criação do tanque jogador caso o teclado foi primida(Ou seja se foi primida uma 
;tecla que gera ação) caso contrário o programa sera prendida no teclado.
;-----------------------------------------------------------------------------
cicloPrincipal:
			  MOV R7,matei2             ;Insere o valor do endereço do estado de
			  ;vida dos tanques Inimigos
			  MOV r0,0                  ;Insere zero no registo 0
			  MOV [r7],r0               ;Actualiza com zero no endereço do r7
			  MOV R5,FizEsperarTeclado  ;Insere Endereço no registro 5 
                           MOV R4,0                  ;Insere zero no registo 4 
                          MOV [R5],R4               ;Actualiza com zero no Endereço do r7
			  CALL pTeclado             ; Chama o processo do teclado
			  MOV R10,BUFFER            ;Insere Endereço no registro 10(Tecla do teclado) 
			  MOVB R7,[R10]            ; Insere no R7 o valor primida no teclado
              MOV R4,4H                ;Insere quatro no registo 4
              CMP R7,R4                ;Compara se o valor primida no teclado é quatro
              JZ chama                 ;Se for então pula para etiqueta Chama
              MOV R4,6H                ;Insere seis(6) no registo 4
              CMP R7,R4                ;Compara se o valor primida no teclado é seis
              JZ chama                 ;Se for então pula para etiqueta Chama
              MOV R4,1H                ;Insere um(1) no registo 4
              CMP R7,R4                ;Compara se o valor primida no teclado é um(1)
		      JZ chama                 ;Se for então pula para etiqueta Chama
		      MOV R4,9H                ;Insere nove(9) no registo 4
		      CMP R7,R4				   ;Compara se o valor primida no teclado é 9
		      JZ chama                 ;Se for então pula para etiqueta Chama
		      MOV R4,0BH               ;Insere B no registo 4
		      CMP R7,R4                ;Compara se o valor primida no teclado é B
		      JZ Mata                  ;Se for então pula para etiqueta Mata
		      MOV R4,0FH
		      CMP R7,R4
		      JZ terminouOjogo
		      MOV R4,0DH
		      CMP R7,R4
		      JZ PauseiOJogo
		      JMP cicloPrincipal       ;Volta para o ciclo principal

;		      Etiqueta chama é resonsavel para chamar o tanque jogador Caso o 
;             usuario digitar as teclas de direção(1,4,6,8)

		      chama:
		        CALL pTanqJogador       ;Chama o processo do tanque Jogador
		        JMP cicloPrincipal      ;Volta para o ciclo principal

;		      Etiqueta resonsavel para chamar o processo de Matar os tanques 
;		      inimigos caso o usuario Digitar a tecla (B)

		      Mata:						
		          CALL pdarTiro           ;Chama o processo de dar tiro
			      JMP cicloPrincipal        ;Volta para o ciclo principal

PauseiOJogo:
jmp PausaOJogo
terminouOjogo:
jmp InicializarLimparTudo
;----------------------------------------------------------------------------
;Etiqueta responsavel para a criação do tanque jogador principal, com as suas 
;respectivas direções



;---------------------------------------------------------------------------
pTanqJogador:
			  CALL torpedo
              PUSH R10              ;Armazena o registo da pilha
              PUSH R1               ;Armazena o registo da pilha
              PUSH R2               ;Armazena o registo da pilha
              PUSH R9               ;Armazena o registo da pilha
              PUSH R0               ;Armazena o registo da pilha
              PUSH R8               ;Armazena o registo da pilha
              PUSH R4               ;Armazena o registo da pilha
		      PUSH R6               ;Armazena o registo da pilha
		      PUSH R7               ;Armazena o registo da pilha
		      PUSH R3               ;Armazena o registo da pilha
			  MOV R10,baseVectoresJ ;Insere no Registo  endereços das Direções do
			  ; Jogador
			  MOV R1,[R10]			;Insere o Número da Linha no registro R1
			  MOV R2,[R10+2]        ;Insere o Número da coluna no registo R2
			  MOV R9,[R10+4]		;Insere o numero da coluna da cabeça do Jogador
			  MOV R0,0H             ;Inicializa o contador

;--------------------------------------------------------------------------------
;Etiqueta responsavel para direcionar a chamada do processo em destaque ou seja 
;quando este processo for chamada primeiro entre os processos significa que se quer 
;somente criar o tanque sem dar ela uma instrução para direcionado
;--------------------------------------------------------------------------------
	soUmaVez: 
			  MOV R0,0H           ;Inicializa o contador
			  MOV R10,UNICAVEZ    ;Insere o Endereço do estado da chamada
			  MOV R7,[R10]        ;Insere o valor contida nela no registo
			  CMP R7,1H           ;Compara se o valor nela contida for uma
			  JZ jogadorPrincipal ;Caso for um então se quer simplesmente desenhar
			  JNZ acao            ;Caso contrário direciona-lo na etiqueta das ações

;-----------------------------------------------------------------------------------
;Caso para alem de desenhar se queira tambem direcionar o tanque do jogador Principal
;é necessário chamar esta etiqueta pois ela vai ate ao endereço do teclado consultar 
;se foi digitado algum digito e logo em seguida direciona-lo
;-----------------------------------------------------------------------------------

	acao:     MOV R0,0H           ;Inicializa o contador
			  MOV R10,BUFFER      ;Insere o Endereço do teclado no registo r10
              MOVB R7,[R10]		  ;Insere o valor contido nela no registo R7
              MOV R4,4H           ;Insere quatro(4) no registo 4
              CMP R7,R4			  ;Compara se o valor primida no teclado é quatro
              JZ andarEsquerda    ;Caso for então fara o jogador andar pela esquerda
              MOV R4,6H           ;Insere seis(6) no registo 4
              CMP R7,R4           ;Compara se o valor primida no teclado é seis
              JZ andarDireita     ;Caso for então fara o jogador andar pela direita
              MOV R4,1H           ;Insere um(1) no registo 4
              CMP R7,R4           ;Compara se o valor primida no teclado é um
		      JZ Subir			  ;Caso for então fara o jogador subir
		      MOV R4,9H           ;Insere nove(9) no registo 4
		      CMP R7,R4           ;Compara se o valor primida no teclado é nove
		      JZ descer           ;Caso for então fara o jogador descer
              JMP FIM 			  ;Pula para o fim para retornar a chamada

;---------------------------------------------------------------------------------
        ;Etiqueta que desenha o tanque do jogador principal 6x3 pixel
;---------------------------------------------------------------------------------
	jogadorPrincipal:  
			  MOV R7,UNICAVEZ   ;Insere o Endereço do estado da chamada
              MOV R10,2H        ;Insere o valor dois(2) no registo r10
              MOV [R7],R10		;Actualiza o valor de r10 no endereço de r7
		      MOV R3,6          ;Insere o valor do término do ciclo da criação
		      ADD R0,1H         ;Faz o incremento d contador
		      ADD R2,1H         ;Incrementa a coluna onde sera pintada o pixel
		      MOV R7,01FH		;Termino da contagem da coluna
		      CMP R7,R2         ;Compara se excedeu dos limites
		      JN Verifica		;se excedeu pula para a inicialização da coluna
		      CMP R3,R0 		;Compara se os pixeis formado ja conclui
		      JN passoC         ;se passar de 6 pixel então pule para formar a cabeca
		      Mov r7,QualLocalizacaoArmazenar
			  MOV R8,1
		      MOV [R7],R8
		      CALL pixel_xy     ;Chama a função de criar pixel
		      JMP jogadorPrincipal ;Pula para criar novamente

	passoC:       
			MOV R0,0              ;Inicializa o contador
		    MOV R7,baseVectoresJ  ;Insere no Registo  endereços das Direções do 
		    ;Jogador
            MOV R1,[R7]           ;Insere o Número da Linha no registro R1

;Etiqueta responsavel por criar a cabeça o jogador principal

	desenhaCabeca:
			  MOV R3,2H           ;Insere o valor do término do ciclo da criação
			  SUB R1,1H           ;Subtrai o valor da linha
			  ADD R0,1H           ;Incrementa o contador
			  MOV R2,R9			  ;Insere o valor da coluna da cabeça no registo r2
			  CMP R3,R0           ;Compara se os pixeis formado ja conclui
			  JN Cabeca           ;se passar de 2 pixel então pule para formar o traço da cabeca
			  Mov r7,QualLocalizacaoArmazenar
			  MOV R8,1
		      MOV [R7],R8
			  CALL pixel_xy       ;Chama a função de criar pixel
			  JMP desenhaCabeca   ;Pula para criar novamente

	Cabeca:		  
			  ADD R1,1H           ;Adiciona mais um na linha
    		  SUB R9,1H           ;subtrai a coluna da cabeça
		      MOV R2,R9           ;Insere o valor da coluna da cabeça no registo r2
		      Mov r7,QualLocalizacaoArmazenar
			  MOV R8,1
		      MOV [R7],R8
			  CALL pixel_xy       ;Chama a função de criar pixel
			  MOV R0,0H           ;Inicializa o contador
		      JMP FIM             ;Pula para o fim para retornar a chamada

	Verifica:
             JMP inicializarColuna ;Inicializa a coluna do jogador


			
				   


;Etiqueta para andar a direita

	andarDireita: 
            MOV R10,0                 ;Insere zero no registro r10.
            MOV R7,localizaPixelJ
			CALL apagaPixel				  ;Chama a etiqueta que apaga os pixel do jogador
			MOV R7,baseVectoresJ      ;Insere no Registo  endereços das Direções do Jogador  
			MOV R1,[R7]               ;Insere o Número da Linha no registro R1
			MOV R2,[R7+2]             ;Insere o Número da coluna no registo R2
			MOV R9,[R7+4]             ;Insere o numero da coluna da cabeça do Jogador
			ADD R2,1H                 ;Incrementa a coluna
			ADD R9,1H                 ;Incrementa a coluna da cabeça
			MOV [R7+4],R9			  ;Actualiza o valor da coluna da cabeça
			MOV [R7+2],r2             ;Actualiza o valor da coluna
			MOV R0,0H                 ;Inicializa o contador
			MOV R10,BUFFER            ;Insere o valor do endereço da tecla primida
			MOVB [R10],R0             ;Actualiza com zero no endereço do teclado
			JMP jogadorPrincipal      ;Volta desenhar o tanque jogador


;Etiqueta para andar a esquerda

	andarEsquerda:
			MOV R10,0                 ;Insere zero no registro r10.
			MOV R7,localizaPixelJ
			CALL apagaPixel               ;Chama a etiqueta que apaga os pixel do jogador
			MOV R7,baseVectoresJ      ;Insere no Registo  endereços das Direções do Jogador  
			MOV R1,[R7]               ;Insere o Número da Linha no registro R1
			MOV R2,[R7+2]             ;Insere o Número da coluna no registo R2
			MOV R9,[R7+4]             ;Insere o numero da coluna da cabeça do Jogador
			SUB R2,1H                 ;Subtrai a coluna 
			SUB R9,1H                 ;Subtrai a coluna da cabeça
			MOV R10,0                 ;Insere zero no registro r10.
			CMP R2,R10                ;compara se a coluna ja esta no começo
			                          ;se estiver então reinicia a coluna
			JN inicializarColuna      ;se estiver então reinicia a coluna
			MOV [R7+4],R9             ;Senao Actualiza as direções da coluna da cabeça 
			MOV [R7+2],r2             ;Actualiza a coluna do jogador
			MOV R0,0H                 ;Inicializa a contagem
			MOV R10,BUFFER            ;Insere o valor do endereço da tecla primida
			MOVB [R10],R0             ;Actualiza com zero no endereço do teclado
			JMP jogadorPrincipal      ;Volta desenhar o tanque jogador

;Etiqueta para andar a cima
	
	Subir:      
			MOV R10,0                 ;Insere zero no registro r10.
			MOV R7,localizaPixelJ
			CALL apagaPixel               ;Chama a etiqueta que apaga os pixel do jogador
			MOV R7,baseVectoresJ      ;Insere no Registo  endereços das Direções do jogador
			MOV R1,[R7]               ;Insere o Número da Linha no registro R1
			MOV R2,[R7+2]             ;Insere o Número da coluna no registro R2
			MOV R9,[R7+4]             ;Insere o Número da coluna da cabeça no registro R9
			SUB R1,1H                 ;Subtrai a linha
			MOV [R7],R1               ;Actualiza a linha
			MOV R0,0H                 ;Reinicia o contador
			MOV R10,BUFFER			  ;Insere o valor do endereço da tecla primida
			MOVB [R10],R0             ;Actualiza com zero no endereço do teclado
			MOV R10,2H                ;Insere dois no registro r10
			CMP R1,R10				  ;Compara se a linha é DOIS
			JNZ jogadorPrincipal	  ;Caso não for então vai desenhar o jogador com novas direções
			JNN jogadorPrincipal      ;Caso não for então vai desenhar o jogador com novas direções
			CALL comeco               ;Caso for então ira reiniciar as direções
			JMP jogadorPrincipal      ;Volta desenhar o tanque jogador

;Etiqueta para andar a baixo


	descer:    
		    MOV R10,0                 ;Insere zero no registro r10.
		    MOV R7,localizaPixelJ
			CALL apagaPixel               ;Chama a etiqueta que apaga os pixel do jogador
			MOV R7,baseVectoresJ      ;Insere no Registo  endereços das Direções do jogador
			MOV R1,[R7]               ;Insere o Número da Linha no registro R1
			MOV R2,[R7+2]             ;Insere o Número da coluna no registro R2
			MOV R9,[R7+4]             ;Insere o Número da coluna da cabeça no registro R9
			ADD R1,1H                 ;Incrementa a linha
			MOV [R7],R1               ;actualiza a linha
			MOV R0,0                  ;Inicializa o contador
			MOV R10,BUFFER            ;Insere o valor do endereço da tecla primida
			MOVB [R10],R0             ;Actualiza com zero no endereço do teclado
			JMP jogadorPrincipal      ;Volta desenhar o tanque jogador


	pixel_xy: 
   		    MOV R4,4                 
		    MOV R6,8
		    MOV R7,R2
            MUL R4,R1
            DIV R7,R6
            ADD R4,R7
	        MOV R7, pixelsMatriz
		    ADD R4,R7
		    MOV R6,7H
		    CMP R2,R6
		    JLE bitpixel
		    MOV R6,0FH
		    CMP R2,R6
		    JLE bitpixel
		    MOV R6,17H
		    CMP R2,R6
		    JLE bitpixel
		    MOV R6 , 1FH

				
	bitpixel:
		    SUB R6,R2
		    MOV R5,ptable
		    ADD R5,R6
		    MOVB R3,[R5]
		    MOVB R6,[R4]
		    OR R6,R3
		    MOVB [R4],R6
		    Mov r7,QualLocalizacaoArmazenar
		    MOV R8,[R7]
		    CMP R8,1H
		    JZ armazenalocalizaPixelJ
		    CMP R8,2
		    JZ armazenaLocalizaPixelIn1
            CMP R8,3
            jz armazenaLocalizaPixelIn2
            CMP R8,4
            JZ armazenaLocalizaPixelTorpedo
            CMP R8,5
            JZ armazenapixelB2
            CMP R8,6
            JZ armazenapixelB1
            ret
            armazenapixelB1:
            		MOV R9,pixelB1
					MOV [R9],R4
					ret
            armazenapixelB2:
            		MOV R9,pixelB2
					MOV [R9],R4
					ret

            armazenaLocalizaPixelTorpedo:
					MOV R10,-2
					MOV R7,LocalizaPixelTorpedo
					CALL guardasAsLocalizacoesDosPixelTorpedo
					RET
		    armazenalocalizaPixelJ:
		    		MOV R10,-2
		    		MOV R7,localizaPixelJ
		    		CALL EtlocalizaPixelJ
		    		RET
		    armazenaLocalizaPixelIn1:
					MOV R10,-2
					MOV R7,LocalizaPixelIn1
					CALL EtLocalizaPixelIn1
		            RET
            armazenaLocalizaPixelIn2:
            		MOV R10,-2
					MOV R7,LocalizaPixelIn2
					CALL EtLocalizaPixelIn2
		   			RET


;Esta etiqueta armazena as todas as localizações dos pixel dos jogadores

	EtlocalizaPixelJ:
			 ADD R10,2               ;Incrementa ao endereçamento
			 MOV r8,[R7+R10]         ;Insere no registo r8 o valor contido no endereco dos pixel(sendo ela uma localização)
			 or r8,r8			     ;Verifica se ja esta no fim
			 JNZ EtlocalizaPixelJ    ;Caso não seja o fim volte a percorrer
			 MOV [R7+R10],r4         ;Caso seje adicione o novo endereço no vector
			 RET                     ;Retorne


;Esta etiqueta armazena todas as direções do tanue jogador sendo ela(a linha e a altura)

	inicioVectorJ:
			  MOV R7,baseVectoresJ    ;Insere no Registo  endereços das Direções do jogador
			  MOV [R7],R1             ;Actualiza a linha
			  MOV [R7+2],r2           ;Actualiza a coluna
			  MOV [R7+4],R9           ;Actualiza a coluna da cabeça
			  RET                     ;Retorne

;Esta etiqueta inicializa a coluna inicias do jogador principal

	inicializarColuna: 
				Mov r10,0
				MOV R7,localizaPixelJ
			    CALL apagaPixel           ;Chama a etiqueta que apaga os pixel do jogador
				MOV R7,baseVectoresJ  ;Insere no Registo  endereços das Direções do jogador
				MOV R1,[R7]           ;Insere o Número da Linha no registro R1
				MOV r2,-1H            ;Volta a coluna no inicio
				MOV R9,3H             ;Indica a coluna da cabeça
				MOV R0,0H             ;Inicializa o contador
				CALL inicioVectorJ    ;Chama a etiqueta para registrar os valores 
				JMP jogadorPrincipal  ;Volte a desenhar


;Esta etiqueta reinicia as direções do tanque jogador

	comeco:
				MOV R1,01FH           ;Inicializa a linha  inicial do jogador Pricipal E da cabeca
				MOV R2,0DH            ;Inicializa a coluna inicial do jogador Pricipal
				MOV R9,11H			  ;Inicializa a coluna da cabeca do jogador
				CALL inicioVectorJ    ;Volte a desenhar
				RET                   ;Retorne



	FIM:        
				POP R3               ;Elimina o registo na pilha
				POP R7               ;Elimina o registo na pilha
				POP R6               ;Elimina o registo na pilha
				POP R4               ;Elimina o registo na pilha
				POP R2               ;Elimina o registo na pilha
				POP R9               ;Elimina o registo na pilha
				POP R1               ;Elimina o registo na pilha
				POP R8               ;Elimina o registo na pilha
				POP R0               ;Elimina o registo na pilha
            	POP R10              ;Elimina o registo na pilha
				RET                  ;Retorne

;----------------------------------------------------------------------
;Etiqueta responsável para apagar os pixeis dos jogadores tanto principal como dos inimigos.
;Com o armazenamento das localizações dos seus pixeis foi necessario 
;para apagar os seus pixeis
;--------------------------------------------------------------------

apagaPixel:
 			 MOV R3,0H              
			 MOV R8,[R7+R10]        
			 MOV [R7+R10],r3       
		     ADD R10,2H            
	   	     MOVB [R8],R3           
			 OR R8,R8              
			 JNZ apagaPixel             
			 RET   



;Esta etiqueta faz os carregamentos iniciais do começo da aplicação


Carregamento:
		     	PUSH R0              ;Armazena o registo da pilha
		     	PUSH R1              ;Armazena o registo da pilha
		     	PUSH R10             ;Armazena o registo da pilha
		     	PUSH R2              ;Armazena o registo da pilha
		     	PUSH R9              ;Armazena o registo da pilha
		     	MOV R7,DescerIn1     ;Insere no registo o endereço da condição da chamada da bala
			 	MOV R5,0H            ;Insere zero no registro 5    
             	MOV [R7],R5          ;Actualiza com zero no endereço acima descrito
		     	MOV r1,BUFFER        ;Insere o valor do endereço da tecla primida
		     	MOV r2,0             ;Insere zero no registro 2
		     	MOVb [r1],r2         ;Actualiza o valor do teclado com zero
		     	MOV R1,FizEsperarTeclado ;Insere no registo o endereço da condição de chamada do teclado 
             	MOV R2,0H            ;Insere zero no registo r2
             	MOV [R1],R2          ;Actualiza com zero no endereço em destaque
		     	MOV R10,VDisplayLH   ;Insere no registo o endereço dos valores dos displays 
			 	MOV R0,0             ;Insere zero no registo
			 	MOV [R10],R0         ;Actuliza com zero no endereço do display das unidades 
			 	MOV [R10+2],R0       ;Actuliza com zero no endereço do display das dezenas 
		     	MOV R1,UNICAVEZ      ;Insere no registo o endereço do estado de chamada da criação do jogador
			 	MOV R10,1H           ;Insere um no registo
			 	MOV [R1],R10		 ;Actualiza com um n endereço em destaque
			 	MOV R1,01FH          ;Inicializa a linha  inicial do jogador Pricipal E da cabeca
			 	MOV R2,0DH            ;Inicializa a coluna inicial do jogador Pricipal
			 	MOV R9,11H			  ;Inicializa a coluna da cabeca do jogador
			 	CALL inicioVectorJ    ;Regista os valores das linhas e coluna chamando esta etiqueta
			 	CALL Pbala1           ;Chama a função da criação da primeira bala
			 	CALL Pbala2           ;Chama a função da criação da segunda bala
			 	CALL pInimigo1        ;Chama a função de criação do primeiro tanque inimigo
			 	CALL pInimigo2        ;Chama a função de criação do segundo tanque inimigo
			 	CALL pTanqJogador     ;Chama a função de criação do tanque jogador principal
			 	POP R9                ;Elimina o registo na pilha
			 	POP R2                ;Elimina o registo na pilha
			 	POP R10               ;Elimina o registo na pilha
			 	POP R1                ;Elimina o registo na pilha
			 	POP R0                ;Elimina o registo na pilha  
			 	RET                   ;Retorne


;---------------------------------------------------------------------------------
;Esta etiqueta permite capturar o digito precionado e executar o seu devido comando.
;caso o usuario não digite nenhum valor esta etiqueta preende o usuario ate que digite. ;Em caso que o jogador morra esta etiqueta prende o usuario ate ue digite a letra F 
;para recomeçar o jogo.

                        ;Funciona da seguinte forma: 

;entra-se com o valor de linha no teclado e com a linha especifica é habilitado no ;interior do teclado somente esta linha para ser clicável. caso a saida do teclado for ;zero logo é preciso mudar a linha continuamente ate que se digite um valor na linha ;especificada e retorna o numero de coluna clicada.


;realçando que a linha e a coluna é continua de potencia de base 2(com os expoente ;0,1,2,3) relactivamente a primeira linha e coluna ate quarta linha e coluna
;----------------------------------------------------------------------------------


pTeclado:
				PUSH R5               ;Armazena o registo da pilha
				PUSH R1               ;Armazena o registo da pilha
				PUSH R6               ;Armazena o registo da pilha
				PUSH R3               ;Armazena o registo da pilha
				PUSH R8               ;Armazena o registo da pilha
				PUSH R10              ;Armazena o registo da pilha
	inicio:
				MOV R5,FizEsperarTeclado ;Insere no registo o endereço da condição de chamada do teclado 
				MOV R9,[R5]              ;Insere no registo o valor contida no endereço
				CMP R9,1H                ;Compara se o valor contida no endeço for 1
				JZ pula                  ;Se for então pede-se simplismente para que usuario digite a letra F(para recomeçar o jogo)
    			CALL OuTrosProcessos     ;caso não for um então permite com que as balas se movimentem e os tanques inimigos 
    pula:
    			MOV 	R5, BUFFER	      ;R5 com endereço de memoria BUFFER
				MOV	R1, 1	              ;testar a linha 1
				MOV R6,PIN                ;R6 com endereõ do porto de entrada
				MOV	R2, POUT	          ;R2 com o endereço do porto de saida


;Corpo principal do programa

	ciclo:
				MOVB 	[R2], R1	 ; escrever no porto de saida
				MOVB 	R3, [R6]	 ; ler do porto de entrada
				AND 	R3, R3		 ; afectar as flags (MOVs não afectam as flags)
				JZ 	inicializarLinha ;Se nenhuma tecla foi premida(então testa outras linhas) se foi primida então vai a instução a seguir

				MOV R8,1       ;Insere um no registo r8
				CMP R8,R1      ;Verifica se a tecla primida pertence a primeira linha
				JZ linha1      ;Se sim então pule para verificar a coluna respectiva
				MOV R8,2       ;Insere dois no registo r8
				CMP R8,R1      ;Verifica se a tecla primida pertence a segunda linha
				JZ linha2      ;Se sim então pule para verificar a coluna
				MOV R8,4       ;Insere quatro no registo r8
				CMP R8,R1      ;Verifica se a tecla primida pertence a terceira linha
				JZ linha3      ;Se sim então pule para verificar a coluna
				MOV R8,8       ;Insere oito no registo r8
				CMP R8,R1      ;Verifica se a tecla primida pertence a quarta linha
				JZ linha4      ;Se sim então pule para verificar a coluna

;Esta etiqueta verifica qual coluna e tecla da quarta linha foi primida

	linha4:
				linha4C1:MOV R8,1 ;Insere um(1) no registo r8
				CMP R8,R3         ;Verifica se é a primerira coluna da quarta linha
				JZ EC             ;Se sim então foi primida a tecla C
				JNZ linha4C2      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha4C2:MOV R8,2 ;Insere dois(2) no registo r8
				CMP R8,R3         ;Verifica se é a segunda coluna da quarta linha
				JZ ED             ;Se sim então foi primida a tecla D
				JNZ linha4C3      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha4C3:MOV R8,4 ;Insere quatro(4) no registo r8
				CMP R8,R3         ;Verifica se é a terceira coluna da quarta linha
				JZ EE             ;Se sim então foi primida a tecla E
				JNZ linha4C4      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha4C4:MOV R8,8 ;Insere oito(8) no registo r8
				CMP R8,R3         ;Verifica se é a quarta coluna da quarta linha
				JZ EF             ;Se sim então foi primida a tecla F

;Esta etiqueta verifica qual coluna e tecla da terceira linha foi primida

	linha3:
				linha3C1:MOV R8,1 ;Insere um(1) no registo r8
				CMP R8,R3         ;Verifica se é a primerira coluna da terceira linha
				JZ Eoito          ;Se sim então foi primida a tecla oito(8)
				JNZ linha3C2      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha3C2:MOV R8,2 ;Insere dois(2) no registo r8
				CMP R8,R3         ;Verifica se é a segunda coluna da terceira linha
				JZ Enove          ;Se sim então foi primida a tecla nove(9)
				JNZ linha3C3      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha3C3:MOV R8,4 ;Insere quarto(4) no registo r8
				CMP R8,R3         ;Verifica se é a terceira coluna da terceira linha
				JZ EA             ;Se sim então foi primida a tecla A
				JNZ linha3C4      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha3C4:MOV R8,8 ;Insere oito(8) no registo r8
				CMP R8,R3         ;Verifica se é a quarta coluna da terceira linha
				JZ EB             ;Se sim então foi primida a tecla B

;Esta etiqueta verifica qual coluna e tecla da segunda linha foi primida

	linha2:
				linha2C1:MOV R8,1  ;Insere um(1) no registo r8
				CMP R8,R3          ;Verifica se é a primerira coluna da segunda linha
				JZ Equatro         ;Se sim então foi primida a tecla quatro(4)
				JNZ linha2C2       ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha2C2:MOV R8,2  ;Insere dois(2) no registo r8
				CMP R8,R3          ;Verifica se é a segunda coluna da segunda linha
				JZ Ecinco          ;Se sim então foi primida a tecla cinco(5)
				JNZ linha2C3       ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha2C3:MOV R8,4  ;Insere quatro(4) no registo r8
				CMP R8,R3          ;Verifica se é a terceira coluna da segunda linha
				JZ Eseis           ;Se sim então foi primida a tecla seis(6)
				JNZ linha2C4       ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha2C4:MOV R8,8  ;Insere oito(8) no registo r8
				CMP R8,R3          ;Verifica se é a quarta coluna da segunda linha
				JZ Esete           ;Se sim então foi primida a tecla sete(7)

;Esta etiqueta verifica qual coluna e tecla da primeira linha foi primida

	linha1:
				linha1C1:MOV R8,1 ;Insere um(1) no registo r8
				CMP R8,R3         ;Verifica se é a primerira coluna da primeira linha
				JZ Ezero          ;Se sim então foi primida a tecla zero(0)
				JNZ linha1C2      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha1C2:MOV R8,2 ;Insere dois(2) no registo r8
				CMP R8,R3         ;Verifica se é a segunda coluna da primeira linha
				JZ Eum            ;Se sim então foi primida a tecla um(1)
				JNZ linha1C3      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha1C3:MOV R8,4 ;Insere quatro(4) no registo r8
				CMP R8,R3         ;Verifica se é a terceira coluna da primeira linha
				JZ Edois          ;Se sim então foi primida a tecla dois(2)
				JNZ linha1C4      ;SE NÃO VERIFICA SE FOI UMA OUTRA COLUNA
				linha1C4:MOV R8,8 ;Insere oito(8) no registo r8
				CMP R8,R3         ;Verifica se é a quarta coluna da primeira linha
				JZ Etres          ;Se sim então foi primida a tecla três(3)
				
				Ezero:MOV R10,0H  ;Insere zero(0) no registo r10
				JMP armazena      ;pule para Armazenar zero(0) no endereço do teclado
				Eum:MOV R10,1H    ;Insere um(1) no registo r10
				JMP armazena      ;pule para Armazenar um(1) no endereço do teclado
				Edois:MOV R10,2H  ;Insere dois(2) no registo r10
				JMP armazena      ;pule para Armazenar dois(2) no endereço do teclado
				Etres:MOV R10,3H  ;Insere tres(3) no registo r10
				JMP armazena      ;pule para Armazenar tres(3) no endereço do teclado
				Equatro:MOV R10,4H;Insere quatro(4) no registo r10
				JMP armazena    ;pule para Armazenar quatro(4) no endereço do teclado
				Ecinco:MOV R10,5H  ;Insere cinco(5) no registo r10
				JMP armazena     ;pule para Armazenar cinco(5) no endereço do teclado
				Eseis:MOV R10,6H   ;Insere seis(6) no registo r10
				JMP armazena      ;pule para Armazenar seis(6) no endereço do teclado
				Esete:MOV R10,7H  ;Insere sete(7) no registo r10
				JMP armazena      ;pule para Armazenar sete(7) no endereço do teclado
				Eoito:MOV R10,8H  ;Insere oito(8) no registo r10
				JMP armazena      ;pule para Armazenar oito(8) no endereço do teclado
				Enove:MOV R10,9H  ;Insere nove(9) no registo r10
				JMP armazena     ;pule para Armazenar nove(9) no endereço do teclado
				EA:MOV R10,0aH   ;Insere Dez(A) no registo r10
				JMP armazena     ;pule para Armazenar Dez(A) no endereço do teclado
				EB:MOV R10,0bH   ;Insere 11(B) no registo r10
				JMP armazena     ;pule para Armazenar 11(B) no endereço do teclado
				EC:MOV R10,0cH   ;Insere 12(C) no registo r10
				JMP armazena     ;pule para Armazenar 12(C) no endereço do teclado
				ED:MOV R10,0dH   ;Insere 13(D) no registo r10
				JMP armazena     ;pule para Armazenar 13(D) no endereço do teclado
				EE:MOV R10,0eH   ;Insere 14(E) no registo r10
				JMP armazena    ;pule para Armazenar 14(E) no endereço do teclado
				EF:MOV R10,0fH  ;Insere 15(F) no registo r10

;Esta etiqueta armazena a tecla digitada na memoria

	armazena:
				MOV	R4, R10		    ;guardar tecla premida em registo
				MOVB 	[R5], R10	; guarda tecla premida em mem�ria
				JMP FIMi


;Esta etiqueta verifica se a linha passou a quarta caso for então volte a primeira

	ExcedeuAlinha:
				cmp r8,r1   ;Compara se o valor contido no r1(linha) excedeu o valor de termino de linha contida no r8(8)
				JN reiniciaAcontagemDaLinha      ;Caso exceder logo volte ao começo
				JNn ciclo                        ;se não volte a testar outras linhas
 
 	reiniciaAcontagemDaLinha:
 				JMP inicio

;Esta etiqueta multiplica a linha em destaque por 2 para testa-la na pressão do teclado caso esceder reinicia a linha em 1

	inicializarLinha:
				MOV R8,2        ;Insere dois(2) no registo r8
 				MUL R1,R8       ;Multiplica o registro r1 por dois(2)
				MOV R8,8        ;Insere o termino do produto(sendo ela termino da potenca das linhas o valor 2 elevado a 3 )
				JMP ExcedeuAlinha   ;Se exceder então volte a primeira linha

	FIMi: 
				POP R10             ;Elimina o registo na pilha
				POP R8              ;Elimina o registo na pilha
				POP R3              ;Elimina o registo na pilha
				POP R6              ;Elimina o registo na pilha
				POP R1              ;Elimina o registo na pilha
				POP R5              ;Elimina o registo na pilha
				RET                 ;Retorne

;Esta Etiqueta permite que enquanto verifica-se qual coluna da linha x foi primida ela dá ações as balas e tanques inimigos

OuTrosProcessos:
    			CALL Pbala1      ;chama o processo de criação da bala1  
    			CALL FoiMatado   ;Verifica se o tanque inimigo matou o tanque jogador
    			CALL Pbala2      ;chama o processo de criação da bala2  
				MOV R5,DescerIn1 ;Insere no registo o endereço da condição da chamada dos tanques inimigo
				MOV R4,1H        ;Insere 1 no registro 4
				MOV [R5],R4      ;Actualiza com zero no endereço acima descrito
				CALL pInimigo1   ;chama o processo de criação do tanque inimigo1
				MOV R5,DescerIn1 ;Insere no registo o endereço da condição da chamada dos tanques inimigo
				MOV R4,1H        ;Insere 1 no registro 4
				MOV [R5],R4      ;Actualiza com zero no endereço acima descrito
				CALL pInimigo2   ;chama o processo de criação do tanque inimigo2
    			RET              ;Retorne


;Esta etiqueta insere no display das unidades e dezenas as cotações do jogador principal. está etiqueta so é chamada caso o tanque mate uns dos tanques inimigos




pDisplay:
	PUSH R9          ;Armazena o registo da pilha
	PUSH R0          ;Armazena o registo da pilha
	PUSH R6          ;Armazena o registo da pilha
	PUSH R5          ;Armazena o registo da pilha
	PUSH R1          ;Armazena o registo da pilha
	PUSH R2          ;Armazena o registo da pilha
	PUSH R3          ;Armazena o registo da pilha
	PUSH R7          ;Armazena o registo da pilha
	MOV R9,VDisplayLH;Insere no registo o valor do endereço os valores dos display(unidades e dezenas)
	MOV R0,[R9]      ;Insere no registo o valor do display das unidades 
	MOV R6,[R9+2]    ;Insere no registo o valor do display das dezenas 
	MOV R5,0Ah       ;Insere no registo o valor de termino de contagem para reinicia a zero o display das unidades

	loop:	
				ADD	R0, 1H       ;Adiciona o registro com o valor das unidades
				MOV [R9],R0      ;Actualiza a memoria com endereço do valor das unidades 
				CMP R5,R0        ;comparase excedeu a 9
				JZ passa         ;caso excedeu então vai somar as dezenas
				JN passa         ;caso excedeu então vai somar as dezenas
				CALL Unidades    ;actualizar o display das unidades
				CALL Dezenas     ;actualizar o display das dezenas
				POP R7           ;Elimina o registo na pilha
				POP R3           ;Elimina o registo na pilha
				POP R2           ;Elimina o registo na pilha
				POP R1           ;Elimina o registo na pilha
				POP R5           ;Elimina o registo na pilha
				POP R6           ;Elimina o registo na pilha
				POP R0           ;Elimina o registo na pilha
				POP R9           ;Elimina o registo na pilha
				RET              ;retorne
	passa:
				MOV r0,0         ;Insere zero no registo r0
	    		MOV [r9],r0      ;Actualiza com zero nas undades 
	    		CALL Unidades    ;chama a função para alterar o valor no display das unidades
				ADD	R6, 1         ;Adiciona o registro com o valor das dezenas
				MOV [R9+2],R6     ;Actualiza a memoria com endereço do valor das dezenas
				CALL Dezenas      ;chama a função para alterar o valor no display das dezenas 
				POP R7            ;Elimina o registo na pilha
				POP R3            ;Elimina o registo na pilha
				POP R2            ;Elimina o registo na pilha
				POP R1            ;Elimina o registo na pilha
				POP R5            ;Elimina o registo na pilha
				POP R6            ;Elimina o registo na pilha
				POP R0            ;Elimina o registo na pilha
				POP R9            ;Elimina o registo na pilha
				RET               ;Retorne

	Unidades:	MOV	R1, nibble_3_0	; máscara para isolar os 4 bits de menor peso
				AND	R0, R1		; limita valor de entrada a valores entre 0 e FH
				MOV	R2, imagem_hexa	; endereço da imagem dos displays hexadecimais	
				MOVB	R3, [R2]	; lê imagem dos displays na memória
				MOV	R1, nibble_7_4	; máscara para isolar os bits 7 a 4 (display High)
				AND	R3, R1		; elimina o valor anterior do display Low
				OR	R3, R0		; junta o novo valor do display Low (bits 3 a 0)
				MOVB	[R2], R3	; actualiza imagem dos displays na memória
				MOV	R1, displays	; endereço dos displays hexadecimais
				MOVB	[R1], R3	; actualiza displays
				RET	; regressa


	Dezenas:MOV R7,R6	
				MOV	R1, nibble_3_0	; máscara para isolar os 4 bits de menor peso
				AND	R7, R1		; limita valor de entrada a valores entre 0 e FH
				SHL	R7, 4		; desloca valor para ficar já nos bits 7 a 4
				MOV	R2, imagem_hexa	; endereço da imagem dos displays hexadecimais	
				MOVB	R3, [R2]	; lê imagem dos displays na memória
				MOV	R1, nibble_3_0	; máscara para isolar os bits 3 a 0 (display Low)
				AND	R3, R1		; elimina o valor anterior do display High
				OR	R3, R7		; junta o novo valor do display High (bits 7 a 4)
				MOVB	[R2], R3	; actualiza imagem dos displays na memória
				MOV	R1, displays	; endereço dos displays hexadecimais
				MOVB	[R1], R3	; actualiza displays 
				RET		; regressa		



descer2:
		JMP descerIni1
;--------------------------------------------------------------------------------
;Esta etiqueta desenha os tanques inimigos com as suas direções ou seja suas movimentações
;----------------------------------------------------------------------------------
pInimigo1:

				MOV R7,conta       ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
				MOV R0,1H          ;Insere um(1) no registro
				MOV [R7],R0        ;Actualiza o valor da contagem no endereçosss
				MOV R7,DescerIn1   ;Insere no registo o endereço da condição da chamada dos tanques inimigo(pra descer)
				MOV R5,[R7]        ;Insere no registo o valor contido no endeço
				CMP R5,1H          ;Compara se o valor contido é um
				JZ descer2          ;Caso for um significa que se quer que os tanques se movimentam

;Esta etiqueta serve como o estagio de formação dos pixels do tanque inimigo

	contagem:	MOV R1,conta       ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
		 		MOV R0,[R1]        ;Insere no registo o valor contido no endereço conta
		 		CMP R0,1H          ;Compara se o valor da contagem é um(1)
		 		JZ UM              ;Caso for pretende-se formar as primeiras linhas dos pixels
		 		CMP R0,2H          ;Compara se o valor da contagem é dois(2)
		 		JZ DOIS            ;Caso for pretende-se formar as segundas linhas dos pixels
		 		CMP R0,3H          ;Compara se o valor da contagem é três(3)
		 		JZ TRES            ;Caso for pretende-se formar as terceiras linhas dos pixels
		 		JMP passo          ;caso exceder pretende-se formar a cabeça do tanque inimigo

;Esta etiqueta forma a primeira linha dos pixeis do tanque inimigo

	UM:			MOV R1,ondePara       ;Insere o endereço da paragem dos pixeis a serem formado
   				MOV R0,8H             ;Insere 8 no registo(valor do termino da quantidade de pixel da primeira linha)
   				MOV [R1],R0           ;Insere 8 no endereço  em destaque
   				MOV R1,conta          ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
   				MOV R0,2H             ;Insere dois(2) no registro
   				MOV [R1],R0           ;Actualiza com dois no endereço da contagem
   				MOV R10,BaseVectorIn1 ;Insere no registo o endereço das cordenas do tanque inimigo1
   				MOV R1,[R10]          ;Insere o valor da linha no registo do tanque inimigo1
   				MOV R2,[R10+2]        ;Insere o valor da coluna no registo do taque inimigo1
   				MOV R0,0H             ;Inicializa o contador
   				JMP Inimigo1          ;Pula para formar o tanque


;Esta etiqueta forma a segunda linha dos pixeis do tanque inimigo

	DOIS:		MOV R1,ondePara       ;Insere o endereço da paragem dos pixeis a serem formado
     			MOV R0,6H              ;Insere 6 no registo(valor do termino da quantidade de pixel da segunda linha)
     			MOV [R1],R0            ;Insere 6 no endereço  em destaque
     			MOV R1,conta           ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
     			MOV R0,3H              ;Insere tres(3) no registro
     			MOV [R1],R0            ;Actualiza com tres no endereço da contagem
     			MOV R10,BaseVectorIn1  ;Insere no registo o endereço das cordenadas do tanque inimigo1 
     			MOV R1,[R10]           ;Insere o valor da linha no registo do tanque inimigo1
     			MOV R2,[R10+2]         ;Insere o valor da coluna no registo do tanque inimigo1
    			ADD R1,1H              ;adiciona a linha para formar a linha asseguir
     			ADD R2,1H              ;adiciona a coluna como o desenho proposto
     			MOV R0,0H              ;Inicializa o contador
     			JMP Inimigo1           ;Pula para formar o tanque

;Esta etiqueta forma a terceira linha dos pixeis do tanque inimigo

	TRES: 		MOV R1,ondePara        ;Insere o endereço da paragem dos pixeis a serem formado
      			MOV R0,4H              ;Insere 4 no registo(valor do termino da quantidade de pixel da terceira linha)
      			MOV [R1],R0            ;Insere 4 no endereço  em destaque
      			MOV R1,conta           ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
      			MOV R0,4H              ;Insere quatro(4) no registro
      			MOV [R1],R0            ;Actualiza com quatro no endereço da contagem
      			MOV R10,BaseVectorIn1  ;Insere no registo o endereço das cordenadas do tanque inimigo1 
      			MOV R1,[R10]           ;Insere o valor da linha no registo do tanque inimigo1
      			MOV R2,[R10+2]         ;Insere o valor da coluna no registo do tanque inimigo1
      			ADD R1,2H              ;adiciona a linha para formar a linha asseguir
      			ADD R2,2H              ;adiciona a coluna como o desenho proposto
      			MOV R0,0H              ;Inicializa o contador
      			JMP Inimigo1           ;Pula para formar o tanque

;Esta etiqueta permite fazer os preprocessameto(inicialização de certos registos) para formar a cabeça do primeiro inimigo

	passo: 
		   		MOV R10,BaseVectorIn1  ;Insere no registo o endereço das cordenadas do tanque inimigo1 
	   			MOV R2,[R10+2]         ;Insere o valor da coluna no registo do tanque inimigo1
	   			ADD R2,3H              ;adiciona a coluna como o desenho proposto
	   			MOV r1,[r10]           ;Insere o valor da linha no registo do tanque inimigo1         
	   			MOV r0,0               ;Inicializa o contador

;Esta etiqueta forma a cabeça do inimigo 1

	CABECA:     MOV R7,2H             ;Insere dois no registo 
	   			sub r1,1h             ;Subtrai a linha
	   			ADD r0,1h             ;Adiciona o contador
	   			cmp r7,r0             ;compara se ja formou a cabeça
	   			JN FIMin1             ;Se ja retorna
	   			Mov r7,QualLocalizacaoArmazenar
			    MOV R8,2
		        MOV [R7],R8
	   			CALL pixel_xy      ;se não cria os pixeis
	   			JMP CABECA            ;Volte a formar a cabeça

;Esta etiqueta forma o corpo do inimigo

	Inimigo1:   MOV R9,ondePara      ;Insere o endereço da paragem dos pixeis a serem formado          
				MOV R7,[R9]          ;Insere no registo o valor contida no endereço
				ADD R0,1H            ;incremento no contador
				ADD R2,1H            ;Adiciona a coluna
				CMP R7,R0            ;compara se ja terminou a formação
				JN contagem          ;se passar a quantidade de pixels desejado então pule PARA FORMAR A cabeca
			    Mov r7,QualLocalizacaoArmazenar
			    MOV R8,2
		        MOV [R7],R8
				CALL pixel_xy        ;chama a função de criar pixel
				JMP Inimigo1         ;Volte a formar o tanque


;Esta etiqueta armazena os todas as localizações dos pixel do tanue inimigo1
;tendo o começo das localizações dos pixeis, esta etiqueta percorre todas as 
;localizações ate encontrar uma localização nula, logo em seguida insere a nova 
;localização

	EtLocalizaPixelIn1:ADD R10,2       ;Adiciona a contagem do endereçamento
				MOV r8,[R7+R10]        ;Insere no registo o valor da localização do pixel
				or r8,r8               ;Veririca se ja chegou a fim
				JNZ EtLocalizaPixelIn1 ;Se não volte ate que chegue
				MOV [R7+R10],r4        ;Depois de chegar armazena na ultima posição o valor da nova localização
				RET                     ;Retorne

;Esta etiqueta cadastra as cordenadas no endereço das cordenadas

	InicioVectorIn1:
	MOV R7,BaseVectorIn1 ;Insere no registo o endereço das cordenadas do tanque inimigo1 
			  	MOV [R7],R1              ;Cadastra o valor da linha no endereço
			  	MOV [R7+2],r2            ;Cadastra o valor da coluna no endereço
			  	RET                      ;Retorne


;Esta etiqueta permite fazer a movimentação do tanque inimigo1 pra baixo

	descerIni1:     
				MOV R10,0H                ;Insere zero do registo r10
				MOV R7,LocalizaPixelIn1
			    CALL apagaPixel        ;Chama a função que apaga os pixeis
			    MOV R7,DescerIn1          ;Insere no registo o endereço da condição da chamada dos tanques inimigo(pra descer)
			    MOV R5,0H                 ;Insere zero no registo 5
                MOV [R7],R5               ;Actualiza com zero o endereço em destaque
			    MOV R7,conta              ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
			    MOV R0,1H                 ;Insere 1 no registo r0
                MOV [R7],R0               ;Actualiza no endereço de contagem com 1
			    MOV R3,01fH               ;Insere no registo o valor da ultima linha
			    MOV R10,BaseVectorIn1     ;Insere no registo o endereço das cordenadas do tanque inimigo1 
			    MOV R1,[R10]              ;Insere no registo o valor da linha
			    MOV R2,[R10+2]            ;Insere no registo o vaor da coluna
			    ADD R1,1H                 ;Adiciona a linha
			    MOV [R10],R1              ;Actualiza o valor da linha na memoria
			    CMP R3,R1                 ;Compara o valor da linha se ja esta no fim
			    JN recomeca               ;Caso estiver no fim então recomeçe
			    JZ recomeca               ;Caso estiver no fim então recomeçe
			    JMP Inimigo1              ;Pule em formar o tanque inimigo

;Esta etiqueta volta os tanques inimigos no inicio da linha

    recomeca:
                CALL RecomecoInimigo1
				JMP Inimigo1

;Esta etiqueta inicializa o estado inicial do tanque inimigo1

	RecomecoInimigo1:
				MOV R1,-10h          ;Inicializa a linha  inicial do tanque inimigo
				MOV R2,3h            ;Inicializa a coluna inicial do tanque inimigo
				CALL InicioVectorIn1 ;Insere no registo o endereço das cordenadas do tanque inimigo1 
				MOV R7,conta         ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
				MOV R0,1H            ;Insere 1 no registo r0
				MOV [R7],R0          ;Actualiza no endereço de contagem com 1
				RET                  ;Retornes


;Esta etiqurta retorna por onde foi chamada esta função
	FIMin1: 
			RET 



DescerInimigo1:
				JMP descerIni2

;------------------------------------------------------------------------------
;Esta etiqueta faz o desenho do tanque inimigo2 com as suas movimentações
;-------------------------------------------------------------------------------
pInimigo2:

				MOV R7,conta         ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
				MOV R0,1H            ;Insere um(1) no registro
				MOV [R7],R0          ;Actualiza o valor da contagem no endereçosss
				MOV R7,DescerIn1     ;Insere no registo o endereço da condição da chamada dos tanques inimigo(pra descer)
				MOV R5,[R7]          ;Insere no registo o valor contido no endeço e, destaque
				CMP R5,1H            ;Compara se o valor contido é um  
				JZ DescerInimigo1    ;Caso for um significa que se quer que os tanques se movimentam











;Esta etiqueta inicializa o estado inicial do tanque inimigo1

;Esta etiqueta serve como o estagio de formação dos pixels do tanque inimigo2
	contagem1:
				MOV R1,conta        ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
		 		MOV R0,[R1]         ;Insere no registo o valor contido no endereço conta
		 		CMP R0,1H           ;Compara se o valor da contagem é um(1)
		 		JZ UM1              ;Caso for pretende-se formar as primeiras linhas 
		 		CMP R0,2H           ;Compara se o valor da contagem é dois(2)
		 		JZ DOIS1            ;Caso for pretende-se formar as segundas linhas dos pixels
		 		JMP passo1          ;caso não pretende-se formar a cabeça do tanque inimigo2


;*******************************************************************************
         ;Esta etiqueta forma a primeira linha dos pixeis do tanque inimigo2
;*******************************************************************************
	UM1:
				MOV R1,ondePara     ;Insere o endereço da paragem dos pixeis a serem formado
   				MOV R0,6H           ;Insere 6 no registo(valor do termino da quantidade de pixel da primeira linha)
   				MOV [R1],R0         ;Insere 6 no endereço  em destaque
   				MOV R1,conta        ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
   				MOV R0,2H           ;Insere dois(2) no registro
   				MOV [R1],R0         ;Actualiza com dois no endereço da contagem
   				MOV R10,BaseVector2 ;Insere no registo o endereço das cordenas do tanque inimigo2
   				MOV R1,[R10]        ;Insere o valor da linha no registo do tanque inimigo2
   				MOV R2,[R10+2]      ;Insere o valor da coluna no registo do taque inimigo2
   				MOV R0,0H           ;Inicializa o contador
   				JMP Inimigo2        ;Pula para formar o tanque

;***********************************************************************************
          ;Esta etiqueta forma a segunda linha dos pixeis do tanque inimigo
;***********************************************************************************
	DOIS1:		
				MOV R1,ondePara     ;Insere o endereço da paragem dos pixeis a serem formado
     			MOV R0,4H           ;Insere 4 no registo(valor do termino da quantidade de pixel da primeira linha)
     			MOV [R1],R0         ;Insere 4 no endereço  em destaque
     			MOV R1,conta        ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
     			MOV R0,3H           ;Insere tres(3) no registro
     			MOV [R1],R0         ;Actualiza com três no endereço da contagem
     			MOV R10,BaseVector2 ;Insere no registo o endereço das cordenas do tanque inimigo2
     			MOV R1,[R10]        ;Insere o valor da linha no registo do tanque inimigo2
     			MOV R2,[R10+2]      ;Insere o valor da coluna no registo do taque inimigo2
     			ADD R1,1H           ;Adiciona o valor da linha
     			ADD R2,1H           ;Adiciona no valor da coluna
     			MOV R0,0H           ;Inicializa a contagem
     			JMP Inimigo2        ;Pula para formar o tanque


;*************************************************************************************
;Esta etiqueta permite fazer os preprocessameto(inicialização de certos registos) 
;para formar a cabeça do primeiro inimigo2
;*************************************************************************************
	passo1:
	  		 	MOV R10,BaseVector2 ;Insere no registo o endereço das cordenas do tanque inimigo2
	   			MOV R2,[R10+2]      ;Insere o valor da coluna no registo do taque inimigo2
	   			ADD R2,3H           ;Adiciona três no valor da coluna
	   			MOV r1,[r10]        ;Insere o valor da linha no registo
	   			MOV r0,0            ;Inicializa o contador

;*************************************************************************************
                  ;Esta etiqueta desenha a cabeça do tanque inimigo
;************************************************************************************
	CABECA2:	
				MOV R7,2H           ;Insere dois no registo r7(limite da cabeça)
	   			sub r1,1h           ;Substrai 1 no valor da linha
	   			ADD r0,1h           ;Incrementa o contador
	   			cmp r7,r0           ;Compara se ja formou a cabeça
	   			JN FIMin2           ;Caso exceder retorne 
	   			Mov r7,QualLocalizacaoArmazenar
			    MOV R8,3
		        MOV [R7],R8
	   			CALL pixel_xy   ;Caso não forma os pixel
	   			JMP CABECA2         ;Pule para formar

;*************************************************************************************
						 ;Esta etiqueta forma o corpo do inimigo
;*************************************************************************************
	Inimigo2:        
 				MOV R9,ondePara     ;Insere o endereço da paragem dos pixeis a serem formado
				MOV R7,[R9]         ;Insere no registo o valor do termino da formação do pixel
				ADD R0,1H           ;Incrementa o contador
				ADD R2,1H           ;Incrementa da coluna
				CMP R7,R0           ;compara o limite da formação 
				JN contagem1        ;se passar a quantidade dos pixeis então pule PARA FORMAR A cabeca
			    Mov r7,QualLocalizacaoArmazenar
			    MOV R8,3
		        MOV [R7],R8
				CALL pixel_xy       ;chama a função de criar pixel
				JMP Inimigo2        ;Pule para formar

;************************************************************************************
;Esta etiqueta armazena as todas as localizações dos pixel do tanque inimigo2
;tendo o começo das localizações dos pixeis, esta etiqueta percorre todas as 
;localizações ate encontrar uma localização nula, logo em seguida insere a nova 
;localização
;************************************************************************************

	EtLocalizaPixelIn2:ADD R10,2           ;Adiciona o endereçamento
					MOV r8,[R7+R10]        ;Insere no registo as localizações
					or r8,r8               ;Verifica se ja chegou no final
					JNZ EtLocalizaPixelIn2 ;Caso não então volte a percorrer
					MOV [R7+R10],r4        ;Caso encerrar então insere a nova localização na memoria
					RET                    ;Retorne

;**************************************************************************************
			;Esta etiqueta cadastra as cordenadas no endereço das cordenadas
;**************************************************************************************
	InicioVectorIn2:	
					MOV R7,BaseVector2     ;Insere no registo o endereço das cordenas do tanque inimigo2
			  		MOV [R7],R1            ;Cadastra a linha na memoria
			  		MOV [R7+2],r2          ;Cadastra a coluna na memoria
			  		RET                    ;Retorne

;*************************************************************************************
		;Esta etiqueta permite fazer a movimentação do tanque inimigo2 pra baixo
;*************************************************************************************

	descerIni2:   
				   MOV R10,0H              ;Insere zero do registo r10
				   MOV R7,LocalizaPixelIn2
			 	   CALL apagaPixel      ;Chama a função que apaga os pixeis
			 	   MOV R7,DescerIn1        ;Insere no registo o endereço da condição da chamada dos tanques inimigo(pra descer)
			 	   MOV R5,0H               ;Insere zero no registo 5
             	   MOV [R7],R5             ;Actualiza com zero o endereço em destaque
			 	   MOV R7,conta            ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
			 	   MOV R0,1H               ;Insere 1 no registo r0
             	   MOV [R7],R0             ;Actualiza no endereço de contagem com 1
			       MOV R3,01fH             ;Insere no registo o valor da ultima linha
			 	   MOV R10,BaseVector2     ;Insere no registo o endereço das cordenas do tanque inimigo2
			       MOV R1,[R10]            ;Insere no registo o valor da linha
			       MOV R2,[R10+2]          ;Insere no registo o valor da coluna
			 	   ADD R1,1H               ;Adiciona a linha
			       MOV [R10],R1            ;Actualiza o valor da linha na memoria
			       MOV [R10+2],R2          ;Actualiza o valor da coluna na memoria
			       CMP R3,R1               ;Compara o valor da linha se ja esta no fim
			       JN recomeca2            ;Caso estiver no fim então recomeçe
			       JZ recomeca2            ;Caso estiver no fim então recomeçe
			       JMP Inimigo2            ;Pule em formar o tanque inimigo

	recomeca2:
					CALL REcomecoInimigo2
					JMP Inimigo2


;**********************************************************************************
			;Esta etiqueta volta os tanques inimigos no inicio da linha
;**********************************************************************************
	REcomecoInimigo2:MOV R1,-10h          ;Inicializa a linha  inicial do inimigo2 
					MOV R2,010H           ;Inicializa a coluna inicial do inimigo2
					CALL InicioVectorIn2
					MOV R7,conta          ;Insere no registo o endereço da condição da formação dos pixeis do tanques Inimigos
					MOV R0,1H             ;Insere 1 no registo r0
					MOV [R7],R0           ;Actualiza no endereço de contagem com 1
					RET                   ;Retorne
	FIMin2:
 					RET 


caminho:
		CALL RecomecoB1
		JMP Pbala1
;-------------------------------------------------------------------------------------
               ;Esta etiqueta desenha a primeira bala e as suas movimentações
;-------------------------------------------------------------------------------------
Pbala1:
					MOV R1,pixelB1         ;Insere no r1 o valor da localização da bala1
					MOV R0,0H              ;Insere zero no registo
					MOV R7,[R1]            ;Insere no registo o valor contido no endereço(a localização)
					MOVB [R7],R0           ;Apaga o pixel

;**************************************************************************************
						;Esta etiqueta movimenta a bala para baixo
;**************************************************************************************
	descerB1:
					MOV R7,BaseVectorB1    ;Insere no endereço as direções da bala1
					MOV r1,[r7]			   ;Insere o valor da linha da bala1 no registo 
					MOV r2,[r7+2]          ;Insere o valor da coluna da bala1 no registo 
					ADD r1,1h              ;Adiciona a linha
					MOV R0,20H             ;Insere no registo o fim da linha
					CMP R0,R1              ;Compara se a linha ja esta no final
					JZ caminho             ;Se ja volte no principio
					JN caminho             ;Se ja volte no principio
					CALL InicioVectorB1
					Mov r7,QualLocalizacaoArmazenar
			        MOV R8,6
		            MOV [R7],R8
					CALL pixel_xy
					JMP fimBala1
;Esta etiqueta reinicia a posição inicial
	RecomecoB1:
					MOV R1,-1H               ;Insre -1 no registo(valor da linha)
					MOV R2,5H                ;Insre 5 no registo(valor da coluna)
					CALL InicioVectorB1      ;Chama a função de cadastrar as localizações
					RET                      ;Retorne

	InicioVectorB1: MOV R7,BaseVectorB1      ;Insere no endereço as direções da bala1
				  	MOV [R7],R1              ;cadastra a linha da bala na memoria
			  		MOV [R7+2],r2            ;Cadastra o valor da coluna na memoria
			  		RET                      ;Retorne

	fimBala1:
					RET


	caminho1:
				CALL RecomecoB2
				JMP Pbala2















Pbala2:
				MOV R1,pixelB2         ;Insere no r1 o valor da localização do pixel da bala2
				MOV R0,0H              ;Insere zero no registo
				MOV R7,[R1]            ;Insere no registo o valor contido no endereço(a localização) 
				MOVB [R7],R0           ;Apaga o pixel anterior criada
	descerB2:
				MOV R7,BaseVectorB2    ;Insere no endereço as direções da bala2
				MOV r1,[r7]            ;Insere o valor da linha da bala2 no registo 
				MOV r2,[r7+2]          ;Insere o valor da coluna da bala2 no registo 
				ADD r1,1h              ;Adiciona a linha
				MOV R0,20H             ;Insere no registo o fim da linha
				CMP R0,R1              ;Compara se a linha ja esta no final
				JZ caminho1            ;Se ja volte no principio
				JN caminho1            ;Se ja volte no principio
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,5
		        MOV [R7],R8
				CALL InicioVectorB2
				CALL pixel_xy
				JMP fimBala2

	RecomecoB2:
				MOV R1,-1H                ;Insre -1 no registo(valor da linha)
				MOV R2,11H                ;Insre 11h no registo(valor da coluna)
				CALL InicioVectorB2       ;Chama a função de cadastrar as localizações
				RET                       ;Retorne

	InicioVectorB2:
				MOV R7,BaseVectorB2      ;Insere no endereço as direções da bala2
			  	MOV [R7],R1              ;cadastra a linha da bala na memoria
			  	MOV [R7+2],r2            ;Cadastra o valor da coluna na memoria
			  	RET                       ;Retorne
	fimBala2: 
				RET



;------------------------------------------------------------------------------
;Esta etiqueta forma os pixel do torpedo(a bala do jogador principal) alinhando 
;perto do jogador
;-----------------------------------------------------------------------------

torpedo:
				MOV R10,0H               ;Insere zero no registo r10
				MOV R7,LocalizaPixelTorpedo
				call apagaPixel
				;CALL limpaTorpedo        ;Chama a função que elimina torpedo
				MOV R7,baseVectoresJ     ;Insere no Registo  endereços das Direções do jogador
				MOV R1,[R7]              ;Insere o valor da linha do jogador
				MOV R2,[R7+2]            ;Insre o valor da coluna do jogador
				SUB R1,2H                ;Subtrai o valor da linha
				ADD R2,1H                ;Adiciona o valor da linha
				MOV r0,0h                ;inicializa o contador
				CALL InicioVectorT       ;Chama o registo para armazenar as localizações do torpedo



;Esta etiqueta desenha os torpedos


	fazerTorpedo:
				MOV R10,3H               ;Insere o termino dos pixeis do torpedo
		     	ADD R0,1H                ;Adiciona a linha
		     	CMP R10,R0               ;Compara se ja terminou a formação do torprdo
		     	JN FimTorpedo            ;Se ja então retorne
		     	SUB R1,1H                ;Subtrai a linha
		     	Mov r7,QualLocalizacaoArmazenar
			    MOV R8,4
		        MOV [R7],R8
		     	CALL pixel_xy            ;Cria os pixeis
		     	JMP fazerTorpedo        ;Pule pra formar os outros pixeis


	guardasAsLocalizacoesDosPixelTorpedo:
				ADD R10,2                                ;Adiciona o Endereçamento
				MOV r8,[R7+R10]                          ;Insere no registo o endereço da localização dos pixeis do torpedo
				or r8,r8                                 ;Verifica se ja está no fim
				JNZ guardasAsLocalizacoesDosPixelTorpedo ;caso não estiver volte a percorrer
				MOV [R7+R10],r4                           ;caso estiver cadastra a nova localização 
				RET                                       ;Retorne

	InicioVectorT:MOV R7,BaseTorpedo       ;Insere no registo o endereço das localizações do torpedo
			  	MOV [R7],R1                ;Cadastra na memoria a linha do torpedo
			  	MOV [R7+2],r2              ;Cadastra na memoria a coluna do torpedo
			  	RET                        ;Retorne


	FimTorpedo:
				RET







pdarTiro:
				MOV R10,BUFFER
				MOV R0,0
				MOVB [R10],R0
				MOV R10,0H
				MOV R7,LocalizaPixelTorpedo
				call apagaPixel
				MOV R7,BaseTorpedo
				MOV R1,[R7]
				MOV R2,[R7+2]
				SUB R1,1H
				MOV R7,2H
				CMP R1,R7
				JZ FimTiro
				JN FimTiro
				CALL InicioVectorT
				MOV r0,0

	fazerTorpedoT:
				MOV R10,3H
		     	ADD R0,1H
		     	CMP R10,R0
		     	JN pdarTiro
		     	SUB R1,1H
		     	MOV R7,BaseVectorIn1
             	MOV R4,[R7+2]
             	MOV R5,8H
             	ADD R4,R5
             	MOV R5,R2
             	CMP R5,3H
             	JNN veificaSeMatouIn1
             	Mov r7,QualLocalizacaoArmazenar
			    MOV R8,4
		        MOV [R7],R8
		     	CALL pixel_xy
		     	JMP fazerTorpedoT

	veificaSeMatouIn1:
				SUB R5,R4
				JN matasteIn1
				MOV R7,BaseVector2
				MOV R4,[R7+2]
				CMP R2,R4
				JNN matasteIn2
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,4
		        MOV [R7],R8
				CALL pixel_xy
				JMP fazerTorpedoT
	matasteIn1:
				CALL pDisplay
				MOV R10,0H
				MOV R7,LocalizaPixelIn1
				call apagaPixel
				CALL RecomecoInimigo1
				MOV R7,DescerIn1
				MOV R5,0H
				MOV [R7],R5
				CALL pInimigo1
				JMP FimTiro
	matasteIn2:
				CALL pDisplay
				MOV R10,0H
				MOV R7,LocalizaPixelIn2
				call apagaPixel
				CALL REcomecoInimigo2
				CALL pInimigo2
	FimTiro:
				RET










;--------------------------------------------------------------------------------------
;Esta etiqueta verifica se o tanque jogador foi morto pelas balas dos tanques inimigos
;Tendo as localizações do jogador e as das balas fica mais facil saber se as localizações se cruzam
;-------------------------------------------------------------------------------------

FoiMatado: 
				MOV R7,BaseVectorB1;----------;Insere no registo o endreço do valor das localizações da bala1
		   		MOV R2,[R7+2];----------------;Insere no registo o valor da coluna da bala1
		   		MOV R7,baseVectoresJ;---------;Insere no registo o endereço do valor das localizações do tanque do jogador princpal
		   		MOV R4,[R7+2];----------------;Insere no registo o valor da coluna do jogador
		   		CMP R4,R2;--------------------;Compara a coluna do tanque jogador principal e da bala1
		   		JNN VerificaOutro;------------;Se não dar negativo logo não estão na mesma liagem para ser morto pela bala1
		   		ADD R4,6H;--------------------;Caso der negativo soma-se a coluna com seis(6) para saber o espaço oucupado pelo tanque e logo verificar se ele morreu
		   		JMP VeSeJMorreuB1;-------------;Pule para verificar se morreu
;************************************************************************************
       ;Esta etiqueta verifica se o tanque jogador foi morto pela segunda bala
;***********************************************************************************
		   		VerificaOutro:
		   				MOV R7,BaseVectorB2
		   				MOV R2,[R7+2]
		   				CMP R4,R2
		   				JNN volte
		   				ADD R4,6H
		   				CMP R2,R4
		   				JZ VeSeJMorreuB2
		   				JN VeSeJMorreuB2
		   				volte:
		   		RET
	VeSeJMorreuB1:
		     	MOV R7,BaseVectorB1
		     	MOV R1,[R7+2]
		     	CMP R4,R1
		     	JNN MORREU
		     	JZ  MORREU
		     	RET
		     	MORREU:
		     		MOV R7,BaseVectorB1
		     		MOV R1,[R7]
		     		MOV R7,baseVectoresJ
		     		MOV R2,[R7]
		     		CMP R2,R1
		     		JZ InicializarLimparTudo
		     		JN InicializarLimparTudo
		     	RET
	VeSeJMorreuB2:
		     	MOV R7,BaseVectorB2
		     	MOV R1,[R7]
		     	MOV R7,baseVectoresJ
		     	MOV R2,[R7]
		     	CMP R2,R1
		     	JZ InicializarLimparTudo
		     	JN InicializarLimparTudo
		     	RET


	InicializarLimparTudo:
				MOV r1,8000h
				MOV R2,807FH
				MOV R0,0H
	limparTudo: 
				MOVB [R1],R0
				ADD R1,1H
				CMP R2,R1
				JN FimDoJogo
				JMP limparTudo

















FimDoJogo:
	letraF:
				MOV R0,0H
				MOV r1,0eh
				MOV r2,04h
	pauF:MOV R7,6H
				ADD R0,1H
				ADD r1,1h
				CMP R7,R0
				JN saiu1
				CALL pixel_xy
				JMP pauF
	saiu1:
				MOV R0,0H
				MOV r2,03h
				MOV r1,0eh
	PtracoF1:	
				MOV R7,5H
				ADD R0,1H
				ADD r2,1h
				CMP R7,R0
				JN saiu2
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP PtracoF1
	saiu2:
				MOV R0,0H
				MOV r2,03h
				MOV r1,11h
	PtracoF2:
				MOV R7,5H
				ADD R0,1H
				ADD r2,1h
				CMP R7,R0
				JN LetraI
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP PtracoF2
	LetraI:
				MOV R0,0H
				MOV r1,0dh
				MOV r2,0ah
	fazerI:		
				MOV R7,7H
				ADD R0,1H
				ADD r1,1h
				CMP R7,R0
				JN LetraM
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP fazerI
	LetraM:
				MOV R0,0H
				MOV r1,0dh
				MOV r2,0ch
	pauM1:		
				MOV R7,7H
				ADD R0,1H
				ADD r1,1h
				CMP R7,R0
				JN saiu3
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP pauM1
	saiu3:
				MOV R0,0H
				MOV r1,0dh
				MOV r2,0ch
	DiagonalM1:
				MOV r7,4h
				ADD R1,1H
				ADD R2,1H
				ADD R0,1H
				CMP R7,R0
				JN saiu4
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP DiagonalM1
	saiu4:
				MOV r0,0h
				sub R2,2H
	DiagonalM2:
				MOV r7,4h
				SUB R1,1H
				ADD R2,1H
				ADD R0,1H
				CMP R7,R0
				JN saiu5
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP DiagonalM2
	saiu5:		
				MOV r0,0h

	pauM2:		
				MOV R7,7H
				ADD R0,1H
				ADD r1,1h
				CMP R7,R0
				JN espeRETeclado
				Mov r7,QualLocalizacaoArmazenar
			    MOV R8,7
		        MOV [R7],R8
				CALL pixel_xy
				JMP pauM2

	espeRETeclado:
				MOV R1,FizEsperarTeclado
              	MOV R2,1H
              	MOV [R1],R2
              	CALL pTeclado
              	MOV R1 ,BUFFER
              	MOVB R2,[R1]
              	MOV R1,0CH
              	CMP R1,R2
              	JZ limpar
              	JMP espeRETeclado
	limpar:
				MOV r1,8000h
				MOV R2,807FH
				MOV R0,0H
	limpei:
				MOVB [R1],R0
				ADD R1,1H
				CMP R2,R1
				JN recomecaTudo
				JMP limpei

	recomecaTudo:
				MOV r10,0
				mov r7,LocalizaPixelIn1
			 	CALL apagaPixel
			 	MOV r10,0
			 	mov r7,LocalizaPixelIn2
			 	CALL apagaPixel
			 	MOV r10,0
			 	mov r7,localizaPixelJ
			 	CALL apagaPixel
			 	MOV r10,0
			 	mov r7,LocalizaPixelTorpedo
			 	CALL apagaPixel
             	CALL RecomecoInimigo1
             	CALL REcomecoInimigo2
             	CALL RecomecoB1
             	CALL RecomecoB2
			 	CALL Carregamento	
			 	MOV SP,baseDaPilha
			 	JMP cicloPrincipal

PausaOJogo:     MOV R1,FizEsperarTeclado
              	MOV R2,1H
              	MOV [R1],R2
              	call pTeclado
              	MOV R1 ,BUFFER
              	MOVB R2,[R1]
              	MOV R1,0eH
              	CMP R1,R2
              	JZ volteOndeEstavas
              	JMP PausaOJogo

volteOndeEstavas:MOV SP,baseDaPilha
				JMP cicloPrincipal
